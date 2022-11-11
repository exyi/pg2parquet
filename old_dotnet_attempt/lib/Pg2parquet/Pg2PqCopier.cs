using System.Diagnostics;
using System.Runtime.CompilerServices;
using Npgsql;
using Npgsql.Schema;
using NpgsqlTypes;
using Parquet;
using Parquet.Data;

namespace Pg2parquet;

public class Pg2PqCopier
{
	internal IColumnWriter[] ColumnWriters { get; }
	public int BatchSize { get; }

	public Pg2PqCopier(
		IColumnWriter[] columnWriters,
		Schema schema,
		int batchSize = 5000
	)
	{
		ColumnWriters = columnWriters;
		BatchSize = batchSize;
		for (int i = 0; i < columnWriters.Length; i++)
		{
			columnWriters[i].Init(batchSize, schema[i]);
		}
	}

	public static Pg2PqCopier Create(
		IEnumerable<NpgsqlDbColumn> postgresColumns,
		Schema? parquetSchema = null,
		IColumnWriterResolver? columnWriterResolver = null)
	{
		parquetSchema ??= new SchemaUtils().GetParquetSchema(postgresColumns);
		columnWriterResolver ??= IColumnWriterResolver.Default;

		if (parquetSchema.Fields.Count != postgresColumns.Count())
			throw new ArgumentException("Number of columns in the parquet schema must match the number of columns in the PostgreSQL reader");

		var writers = postgresColumns.Zip(parquetSchema.Fields, (p, f) => columnWriterResolver.Resolve(p, f)).ToArray();
		return new Pg2PqCopier(writers, parquetSchema);
	}

	public async Task<int> CopyBatch(ParquetWriter writer, NpgsqlBinaryExporter reader)
	{
		using var group = writer.CreateRowGroup();
		int rowI = 0;
		for (; rowI < BatchSize; rowI++)
		{
			var nCols = reader.StartRow();
			if (nCols <= 0)
			{
				// end of stream
				break;
			}

			for (int colI = 0; colI < nCols; colI++)
			{
				ColumnWriters[colI].WriteValue(reader);
			}
		}

		for (int i = 0; i < ColumnWriters.Length; i++)
		{
			foreach (var column in ColumnWriters[i].GetColumns())
			{
				// Console.WriteLine($"Writing column {column.Field.Name} with {column.Count} rows");
				await group.WriteColumnAsync(column);
			}

			ColumnWriters[i].Reset();
		}

		return rowI;
	}

	public async Task<int> Copy(ParquetWriter writer, NpgsqlBinaryExporter reader)
	{
		int nRows = 0;
		while (true)
		{
			var n = await CopyBatch(writer, reader);
			nRows += n;
			if (n < BatchSize)
			{
				break;
			}
		}
		return nRows;
	}

	public async Task<int> Copy(
		Stream output,
		NpgsqlConnection connection,
		string sqlQuery,
		IColumnWriterResolver? columnWriterResolver = null
	)
	{
		IEnumerable<NpgsqlDbColumn> columns;
		using (var queryCommand = new NpgsqlCommand(sqlQuery, connection))
		using (var reader = queryCommand.ExecuteReader(System.Data.CommandBehavior.SchemaOnly))
		{
			columns = reader.GetColumnSchema();
			reader.Close();
		}

		var schema = new SchemaUtils().GetParquetSchema(columns);

		using var writer = await ParquetWriter.CreateAsync(schema, output);

		using var binaryExporter = await connection.BeginBinaryExportAsync($"COPY ({sqlQuery}) TO STDOUT (FORMAT BINARY)");
		return await this.Copy(writer, binaryExporter);
	}

	public async Task<int> Copy(
		string outputFile,
		NpgsqlConnection connection,
		string sqlQuery,
		IColumnWriterResolver? columnWriterResolver = null
	)
	{
		int result;
		using (var output = File.Create(outputFile))
			result = await Copy(output, connection, sqlQuery, columnWriterResolver);

		// using var reader = await ParquetReader.CreateAsync(outputFile);
		// var r = await reader.ReadEntireRowGroupAsync();
		// foreach (var c in r)
		// {
		// 	Console.WriteLine($"{c.Field.Name} has {c.Count} rows: DL: {string.Join(",", c.RepetitionLevels)}");
		// }
		return result;
	}

}

public interface IColumnWriterResolver
{
	public IColumnWriter Resolve(NpgsqlDbColumn pgColumn, Field parquetField)
	{
		IColumnWriter primitive(Type t, bool isArray, NpgsqlDbType dbType)
		{
			var nnt = Nullable.GetUnderlyingType(t) ?? t;
			var isNullable = t != nnt;
			if (nnt == typeof(DateTime))
				return isArray ? new TimestampWithoutTZArrayColumnWriter() : new TimestampWithoutTZColumnWriter();

			if (nnt == typeof(string))
				return isArray ? new ArrayColumnWriter<string>(dbType) : new PrimitiveColumnWriter<string>(dbType);

			if (nnt == typeof(bool))
				return isArray ? new ArrayColumnWriter<bool?>(dbType) :
					isNullable ? new PrimitiveColumnWriter<bool?>(dbType) :
								 new PrimitiveColumnWriter<bool>(dbType);

			if (nnt == typeof(byte))
				return isArray ? new ArrayColumnWriter<byte?>(dbType) :
					isNullable ? new PrimitiveColumnWriter<byte?>(dbType) :
								 new PrimitiveColumnWriter<byte>(dbType);

			if (nnt == typeof(short))
				return isArray ? new ArrayColumnWriter<short?>(dbType) :
					isNullable ? new PrimitiveColumnWriter<short?>(dbType) :
								 new PrimitiveColumnWriter<short>(dbType);

			if (nnt == typeof(int))
				return isArray ? new ArrayColumnWriter<int?>(dbType) :
					isNullable ? new PrimitiveColumnWriter<int?>(dbType) :
								 new PrimitiveColumnWriter<int>(dbType);

			if (nnt == typeof(long))
				return isArray ? new ArrayColumnWriter<long?>(dbType) :
					isNullable ? new PrimitiveColumnWriter<long?>(dbType) :
								 new PrimitiveColumnWriter<long>(dbType);

			if (nnt == typeof(float))
				return isArray ? new ArrayColumnWriter<float?>(dbType) :
					isNullable ? new PrimitiveColumnWriter<float?>(dbType) :
								 new PrimitiveColumnWriter<float>(dbType);

			if (nnt == typeof(double))
				return isArray ? new ArrayColumnWriter<double?>(dbType) :
					isNullable ? new PrimitiveColumnWriter<double?>(dbType) :
								 new PrimitiveColumnWriter<double>(dbType);

			if (nnt == typeof(decimal))
				return isArray ? new ArrayColumnWriter<decimal?>(dbType) :
					isNullable ? new PrimitiveColumnWriter<decimal?>(dbType) :
								 new PrimitiveColumnWriter<decimal>(dbType);

			if (nnt == typeof(DateTimeOffset))
				return isArray ? new ArrayColumnWriter<DateTimeOffset?>(dbType) :
					isNullable ? new PrimitiveColumnWriter<DateTimeOffset?>(dbType) :
								 new PrimitiveColumnWriter<DateTimeOffset>(dbType);

			if (nnt == typeof(TimeSpan))
				return isArray ? new ArrayColumnWriter<TimeSpan?>(dbType) :
					isNullable ? new PrimitiveColumnWriter<TimeSpan?>(dbType) :
								 new PrimitiveColumnWriter<TimeSpan>(dbType);


			Console.WriteLine($"WARNING: Unsupported type {t} (from dbtype {dbType})");

			if (isArray)
			{
				return (IColumnWriter)Activator.CreateInstance(typeof(ArrayColumnWriter<>).MakeGenericType(t), dbType)!;
			}
			else
			{
				return (IColumnWriter)Activator.CreateInstance(typeof(PrimitiveColumnWriter<>).MakeGenericType(t), dbType)!;
			}
		}
		if (((parquetField as ListField)?.Item ?? parquetField) is DataField dataField && pgColumn.NpgsqlDbType is not (null or NpgsqlDbType.Unknown))
		{
			var isArray = dataField.MaxRepetitionLevel > 0;
			var dbType = pgColumn.NpgsqlDbType.Value & ~NpgsqlDbType.Array;
			Console.WriteLine($"Column {pgColumn.ColumnName}: {dbType} -> {dataField.ClrType}{(isArray ? "[]" : "")} -> {dataField.DataType}");
			return dbType switch {
				NpgsqlDbType.Timestamp => primitive(typeof(DateTime), isArray, dbType),
				_ => primitive(dataField.ClrType, isArray, dbType)
			};
		}
		else if (parquetField is StructField sfield)
		{
			return new TableColumnsWriter();
		}
		else if (parquetField is ListField list && list.Item is StructField)
		{
			return new StructArrayColumnsWriter();
		}
		else
		{
			throw new NotImplementedException($"Unsupported column {parquetField} of type {pgColumn.DataType}");
		}
	}

	public static IColumnWriterResolver Default { get; } = new DefaultColumnWriterResolver();
}

class DefaultColumnWriterResolver : IColumnWriterResolver { }
