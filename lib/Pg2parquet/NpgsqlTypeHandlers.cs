using System.Reflection;
using Npgsql;
using Npgsql.BackendMessages;
using Npgsql.Internal;
using Npgsql.Internal.TypeHandling;
using Npgsql.PostgresTypes;
using Row=Parquet.Data.Rows.Row;

namespace Pg2parquet;

public static class NpgsqlTypeHandlers
{
	public static void RegisterTypeHandlers()
	{
		NpgsqlConnection.GlobalTypeMapper.AddTypeResolverFactory(new CustomTypeResolverF());
	}

	class CustomTypeResolverF : TypeHandlerResolverFactory
	{
		public override TypeHandlerResolver Create(NpgsqlConnector connector)
		{
			return new CustomTypeResolver(connector);
		}

		public override string? GetDataTypeNameByClrType(Type clrType)
		{
			return null;
		}

		public override TypeMappingInfo? GetMappingByDataTypeName(string dataTypeName)
		{
			return null;
		}
	}

	class CustomTypeResolver : TypeHandlerResolver
	{
		private readonly NpgsqlConnector connector;
		private readonly PostgresCompositeType? randomCompositeType;
		private readonly IDictionary<uint, PostgresType> typeByOid;

		public CustomTypeResolver(NpgsqlConnector connector)
		{
			this.connector = connector;
			var compositeTypes =
				(IReadOnlyList<PostgresCompositeType>)typeof(NpgsqlDatabaseInfo).GetProperty("CompositeTypes", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(connector.DatabaseInfo)!;
			this.randomCompositeType = compositeTypes.FirstOrDefault();
			this.typeByOid =
				(IDictionary<uint, PostgresType>)typeof(NpgsqlDatabaseInfo).GetProperty("ByOID", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(connector.DatabaseInfo)!;
		}

		PostgresCompositeType? GetCompositeType(string dataTypeName)
		{
			var t = connector.DatabaseInfo.GetPostgresTypeByName(dataTypeName);
			return t as PostgresCompositeType;
		}

		public override TypeMappingInfo? GetMappingByDataTypeName(string dataTypeName)
		{
			return dataTypeName switch {
				_ when GetCompositeType(dataTypeName) is {} =>
					new TypeMappingInfo(NpgsqlTypes.NpgsqlDbType.Unknown, dataTypeName, typeof(Row)),
				_ => null
			};
		}

		public override NpgsqlTypeHandler? ResolveByClrType(Type type)
		{
			return type == typeof(Row) ? new CustomStructHandler(typeByOid, connector, randomCompositeType!) :
				null;
		}

		public override NpgsqlTypeHandler? ResolveByDataTypeName(string typeName)
		{
			return typeName switch {
				_ when GetCompositeType(typeName) is {} type =>
					new CustomStructHandler(typeByOid, connector, type),
				_ => null
			};
		}
	}

	class CustomStructHandler : NpgsqlTypeHandler<Row>
	{
		private readonly IDictionary<uint, PostgresType> byOid;
		private readonly NpgsqlConnector connector;
		PostgresCompositeType someRandomPgType;
		private readonly object typeMapper;

		public CustomStructHandler(
			IDictionary<uint, PostgresType> byOid,
			NpgsqlConnector connector,
			PostgresCompositeType someRandomPgType) : base(someRandomPgType)
		{
			this.byOid = byOid;
			this.connector = connector;
			this.someRandomPgType = someRandomPgType;

			this.typeMapper = typeof(NpgsqlConnector).GetProperty("TypeMapper", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(connector)!;
			if (typeMapper is null)
				throw new Exception("NpgsqlConnector.TypeMapper does not exist?");

		}

		public override async ValueTask<Row> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription? fieldDescription = null)
		{
			await buf.Ensure(4, async);

			int fcount = buf.ReadInt32();
			if (fcount == -1)
				return null!;

			var fields = new object?[fcount];
			for (int i = 0; i < fcount; i++)
			{
				await buf.Ensure(8, async);
				uint oid = buf.ReadUInt32();
				int fieldLen = buf.ReadInt32();
				if (fieldLen == -1)
				{
					fields[i] = null;
					continue;
				}

				// TODO: this is fucking slow
				var fieldType = byOid[oid];
				var handler = (NpgsqlTypeHandler)typeMapper.GetType().GetMethod("ResolveByOID", BindingFlags.NonPublic | BindingFlags.Instance)!.Invoke(typeMapper, new object[] { oid })!;
				var fieldValue = await handler.ReadAsObject(buf, fieldLen, async);
				fields[i] = fieldValue;
			}
			
			return new Row(fields);
		}

		public override int ValidateAndGetLength(Row value, ref NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter)
		{
			throw new NotImplementedException();
		}

		public override int ValidateObjectAndGetLength(object value, ref NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter)
		{
			throw new NotImplementedException();
		}

		public override Task Write(Row value, NpgsqlWriteBuffer buf, NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter, bool async, CancellationToken cancellationToken = default)
		{
			throw new NotImplementedException();
		}

		public override Task WriteObjectWithLength(object? value, NpgsqlWriteBuffer buf, NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter, bool async, CancellationToken cancellationToken = default)
		{
			throw new NotImplementedException();
		}
	}
}
