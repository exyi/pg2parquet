using System.Collections.ObjectModel;
using Npgsql;
using Npgsql.PostgresTypes;
using Npgsql.Schema;
using NpgsqlTypes;
using Parquet.Data;

namespace Pg2parquet;

public enum UuidTypeHandling {
	Error,
	Bytes,
	String
}
public enum JsonbTypeHandling {
	Error,
	String
}

public enum NumericTypeHandling {
	ToFloat,
	AsDecimal
}

public record SchemaSettings(
	UuidTypeHandling UuidTypeHandling = UuidTypeHandling.String,
	JsonbTypeHandling JsonbTypeHandling = JsonbTypeHandling.String,
	NumericTypeHandling NumericTypeHandling = NumericTypeHandling.AsDecimal
) {
	public static readonly SchemaSettings Default = new();
}

public class SchemaUtils
{
	public SchemaSettings Settings { get; set; } = SchemaSettings.Default;

	// public Npgsql.TypeMapping.INpgsqlTypeMapper PgTypeMapper { get; set; }


	public Schema GetParquetSchema(NpgsqlDataReader reader)
	{
		var columns = reader.GetColumnSchema();

		return GetParquetSchema(columns);
	}

	public Schema GetParquetSchema(IEnumerable<NpgsqlDbColumn> columns)
	{
		var pqCols = columns.Select(c =>
		{
			var x = TranslateField(c.ColumnName, c.AllowDBNull != false, c.PostgresType);
			if (x != null)
				return x;

			throw new NotSupportedException("Column type not supported: " + c.DataTypeName);
		}).ToArray();

		return new Schema(pqCols);
	}

	Field? HandleNumericType(NpgsqlDbColumn col)
	{
		if (Settings.NumericTypeHandling == NumericTypeHandling.ToFloat) {
			return null;
		}
		var t = col.NpgsqlDbType;
		var isArray = (t & NpgsqlDbType.Array) != 0;
		if (isArray) {
			t &= ~NpgsqlDbType.Array;
		}
		if (col.NpgsqlDbType is NpgsqlDbType.Numeric)
		{
			return new DecimalDataField(col.ColumnName, col.NumericPrecision ?? 38, col.NumericScale ?? 18, hasNulls: col.AllowDBNull != false, isArray: isArray);
		}

		if (col.NpgsqlDbType is NpgsqlDbType.Money)
		{
			return new DecimalDataField(col.ColumnName, 19, 4, hasNulls: col.AllowDBNull != false, isArray: isArray);
		}

		return null;
	}



	Field? TranslateField(string name, bool allowNull, PostgresType col)
	{
		return
			HandleTimeTypes(name, allowNull, col) ??
			HandlePrimitiveType(name, allowNull, col) ??
			HandleStructType(name, allowNull, col) ??
			HandleArrayType(name, col);
	}

	Field? HandleTimeTypes(string name, bool allowNull, PostgresType col)
	{
		var isArray = false;
		if (col is PostgresArrayType arrayType) {
			isArray = true;
			col = arrayType.Element;
		}

		if (col.InternalName is "timestamp" or "timestamptz") {
			return new DateTimeDataField(name, DateTimeFormat.DateAndTime, allowNull, isArray);
		}
		if (col.InternalName is "date") {
			return new DateTimeDataField(name, DateTimeFormat.Date, allowNull, isArray);
		}
		if (col.InternalName is "time" or "timetz") {
			return new TimeSpanDataField(name, TimeSpanFormat.MicroSeconds, allowNull, isArray);
		}

		return null;
	}

	Field? HandleArrayType(string name, PostgresType type)
	{
		if (type is not PostgresArrayType arrayType) {
			return null;
		}
		var elementType = arrayType.Element;

		var elementField = TranslateField(name, true, elementType);

		if (elementField is null) {
			return null;
		}

		return new ListField(name, elementField);
	}

	Field? HandleStructType(string name, bool allowNull, PostgresType type)
	{
		var c = type as PostgresCompositeType;
		if (c is null) return null;

		var fields = c.Fields.Select(f => TranslateField(f.Name, true, f.Type)).ToArray();

		return new StructField(name, fields);
	}

	Field? HandlePrimitiveType(string name, bool allowNull, PostgresType col)
	{
		// var t = col.NpgsqlDbType;
		var isArray = false;
		if (col is PostgresArrayType arrayType) {
			isArray = true;
			col = arrayType.Element;
		}
		DataType? mapType(string name) => name switch {
			"text" or "varchar" or "bpchar" => DataType.String,
			"int4" or "tid" or "oid" or "xid" => DataType.Int32,
			"bigint" or "int8" or "xid8" => DataType.Int64,
			"int2" => DataType.Int16,
			"char" => DataType.UnsignedByte,
			"float8" => DataType.Double,
			"float4" => DataType.Float,
			"bool" => DataType.Boolean,
			"bytea" => DataType.ByteArray,
			"uuid" => Settings.UuidTypeHandling switch {
				UuidTypeHandling.Bytes => DataType.ByteArray,
				UuidTypeHandling.String => DataType.String,
				_ => throw new NotSupportedException("UUID support is disabled")
			},
			"jsonb" or "json" => Settings.JsonbTypeHandling switch {
				JsonbTypeHandling.String => DataType.String,
				_ => throw new NotSupportedException("JSONB support is disabled")
			},
			"xml" => DataType.String,
			"time" => DataType.TimeSpan,
			"date" => DataType.DateTimeOffset,
			"macaddr" or "macaddr8" or "inet" or "cidr" => DataType.String,
			"numeric" or "decimal" or "money" => DataType.Double, // fallback for composite types where we don't know the precision
			_ => null
		};

		var dataType = mapType(col.Name) ?? mapType(col.InternalName);

		if (dataType == null) {
			return null;
		}

		// Field f = new DataField(name, dataType.Value, allowNull);
		// if (isArray) {
		// 	f = new ListField(name, f);
		// }
		// return f;
		return new DataField(name, dataType.Value, allowNull, isArray);
	}


	internal static Type WrapClrType(Type t, bool nullable, bool isArray)
	{
		if (nullable && t.IsValueType) {
			t = typeof(Nullable<>).MakeGenericType(t);
		}
		if (isArray) {
			t = t.MakeArrayType();
		}
		return t;
	}
}
