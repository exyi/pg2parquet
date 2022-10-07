using System.Diagnostics;
using Npgsql;
using NpgsqlTypes;
using Parquet.Data;

namespace Pg2parquet;
public abstract class ArrayColumnWriterBase<T> : IColumnWriter
{
	private protected T[] buffer = null!;
	private protected int[] rls = null!;
	private protected int i = 0;
	private protected DataField field = null!;

	public void Init(int batchSize, Field field)
	{
		if (field is ListField lf)
		{
			this.field = (DataField)lf.Item;
		}
		else
		{
			this.field = (DataField)field;
		}
		this.buffer = new T[batchSize];
		this.rls = new int[batchSize];
		Debug.Assert(this.field.MaxRepetitionLevel == 1);
	}

	public void Reset()
	{
		i = 0;
	}

	public IEnumerable<DataColumn> GetColumns()
	{
		var rls2 = new int[i];
		Array.Copy(rls, rls2, i);
		// Console.WriteLine($"Writing {field.Path}: {string.Join(", ", buffer.Take(i))}");
		// Console.WriteLine($"    RLs: {string.Join(", ", rls2)}");
		return new [] { new DataColumn(field, buffer, 0, i, repetitionLevels: rls2) };
	}
	public abstract void WriteValue(NpgsqlBinaryExporter reader);

	protected void WriteEmpty()
	{
		MemUtils.Append(ref rls, i, 0);
		MemUtils.Append(ref buffer, i, default!);
		i++;
	}

	protected void WriteArrayStart(Span<T> array)
	{
		if (array.Length == 0)
		{
			this.WriteEmpty();
			return;
		}
		MemUtils.Append(ref rls, i, 0);
		MemUtils.AppendRange(ref buffer, i, array);
		MemUtils.AppendMultiple(ref rls, i, 1, repeat: array.Length - 1);
		i += array.Length;
	}
}

public sealed class TimestampWithoutTZArrayColumnWriter: ArrayColumnWriterBase<DateTimeOffset?>
{
	public TimestampWithoutTZArrayColumnWriter() { }

	public override void WriteValue(NpgsqlBinaryExporter reader)
	{
		if (reader.IsNull)
		{
			reader.Skip();
			this.WriteEmpty();
			return;
		}
		var d = reader.Read<DateTime[]>();
		var array = new DateTimeOffset?[d.Length];
		for (int i = 0; i < d.Length; i++)
		{
			array[i] = new DateTimeOffset(d[i], TimeSpan.Zero);
		}
		this.WriteArrayStart(array);
	}
}


public sealed class ArrayColumnWriter<T>: ArrayColumnWriterBase<T>
{
	public ArrayColumnWriter(NpgsqlDbType dbType)
	{
		DbType = dbType;
	}

	readonly NpgsqlDbType DbType;


	public override void WriteValue(NpgsqlBinaryExporter reader)
	{
		if (reader.IsNull)
		{
			reader.Skip();
			this.WriteEmpty();
		}
		else
		{
			var array = reader.Read<T[]>(DbType | NpgsqlDbType.Array);
			this.WriteArrayStart(array);
		}
	}
}
