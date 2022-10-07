using System.Diagnostics;
using System.Runtime.CompilerServices;
using Npgsql;
using NpgsqlTypes;
using Parquet.Data;

namespace Pg2parquet;

public sealed class PrimitiveColumnWriter<T> : PrimitiveColumnWriterBase<T>
{
	public PrimitiveColumnWriter(NpgsqlDbType dbType)
	{
		DbType = dbType;
	}
	readonly NpgsqlDbType DbType;

	public override void WriteValue(NpgsqlBinaryExporter reader)
	{
		if (IsNullableType())
		{
			reader.Skip();
			buffer[i] = reader.Read<T>(DbType);
		}
		else
		{
			if (reader.IsNull)
			{
				reader.Skip();
				buffer[i] = default!;
			}
			else
			{
				buffer[i] = reader.Read<T>(DbType);
			}
		}

		i++;
	}
}

public class TimestampWithoutTZColumnWriter: PrimitiveColumnWriterBase<DateTimeOffset?>
{
	public TimestampWithoutTZColumnWriter() { }

	public override void WriteValue(NpgsqlBinaryExporter reader)
	{
		if (reader.IsNull)
		{
			reader.Skip();
			this.Write(null);
		}
		else
			this.Write(new DateTimeOffset(reader.Read<DateTime>(), TimeSpan.Zero));
	}
}

public abstract class PrimitiveColumnWriterBase<T> : IColumnWriter
{
	public PrimitiveColumnWriterBase()
	{
	}
	private protected T[] buffer = null!;
	private protected int i = 0;
	private protected DataField field = null!;

	public void Init(int batchSize, Field field)
	{
		this.field = (DataField)field;
		this.buffer = new T[batchSize];
		Debug.Assert(!this.field.IsArray);
	}

	public IEnumerable<DataColumn> GetColumns()
	{
		return new [] { new DataColumn(field, buffer, 0, i) };
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private protected static bool IsNullableType() => typeof(T).IsValueType && typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>);

	public abstract void WriteValue(NpgsqlBinaryExporter reader);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	protected void Write(T value)
	{
		buffer[i] = value;
		i++;
	}

	public void Reset()
	{
		i = 0;
	}
}
