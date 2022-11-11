using System.Collections;
using Npgsql;
using Parquet.Data;
using Parquet.Data.Rows;

namespace Pg2parquet;

public class TableColumnsWriter : IColumnWriter
{
	RowsToDataColumnsConverter converter = null!;

	public IEnumerable<DataColumn> GetColumns()
	{
		return converter.Convert();
	}

	public void Init(int batchSize, Field field)
	{
		var f = (StructField)field;
		converter = new RowsToDataColumnsConverter(f.Fields.ToArray());
	}

	public void Reset()
	{
		converter.Reset();
	}

	public void WriteValue(NpgsqlBinaryExporter reader)
	{
		if (reader.IsNull)
		{
			converter.WriteRow(null);
			reader.Skip();
		}
		else
		{
			converter.WriteRow(reader.Read<Row>());
		}
	}
}

public class StructArrayColumnsWriter : IColumnWriter
{
	RowsToDataColumnsConverter converter = null!;

	public IEnumerable<DataColumn> GetColumns()
	{
		return converter.Convert();
	}

	public void Init(int batchSize, Field field)
	{
		var f = (StructField)((ListField)field).Item;
		converter = new RowsToDataColumnsConverter(f.Fields.ToArray(), baseLevel: 1);
	}

	public void Reset()
	{
		converter.Reset();
	}

	int valueIndex = 0;

	public void WriteValue(NpgsqlBinaryExporter reader)
	{
		if (reader.IsNull)
		{
			converter.WriteRow(null);
			reader.Skip();
		}
		else
		{
			var lvl = new LevelIndex[] {
				new(0, valueIndex),
				new(1, 0)
			};
			var rows = reader.Read<Row[]>();

			foreach (var r in rows)
			{
				converter.WriteRow(r, lvl);
				lvl[1].Index++;
			}
		}

		valueIndex++;
	}
}


// Copy pasta from parquet-dotnet Table->Column converter
class RowsToDataColumnsConverter
{
	private readonly Field[] schema;
	private readonly int baseLevel;
	private readonly Dictionary<string, DataColumnAppender> pathToDataColumn = new Dictionary<string, DataColumnAppender>();
	private readonly List<Field> flattenedFieldList = new();

	public RowsToDataColumnsConverter(Field[] schema, int baseLevel = 0)
	{
		this.schema = schema;
		this.baseLevel = baseLevel;
	}

	int mainIndex = 0;

	public IEnumerable<DataColumn> Convert(IEnumerable<Row> rows)
	{
		foreach (Row row in rows)
		{
			WriteRow(row);
		}

		return Convert();
	}

	public IEnumerable<DataColumn> Convert()
	{
		return flattenedFieldList
			.Select(df => GetAppender(df).ToDataColumn());
	}

	public void Reset()
	{
		foreach (var c in pathToDataColumn.Values)
			c.Clear();
	}

	public void WriteRow(Row? row, LevelIndex[]? levels = null)
	{
		ProcessRow(schema, row, baseLevel, levels ?? new[] { new LevelIndex(baseLevel, mainIndex) });
		mainIndex++;
	}

	private void ProcessRows(IReadOnlyCollection<Field> fields, IReadOnlyCollection<Row> rows, int level, LevelIndex[] indexes)
	{
		int i = 0;
		foreach (Row row in rows)
		{
			ProcessRow(fields, row, level, indexes.Append(new LevelIndex(level, i++)).ToArray());
		}
	}

	private void ProcessRow(IReadOnlyCollection<Field> fields, Row? row, int level, LevelIndex[] indexes)
	{
		int cellIndex = 0;
		foreach (Field f in fields)
		{
			switch (f.SchemaType)
			{
				case SchemaType.Data:
					ProcessDataValue(f, row?[cellIndex], indexes);
					break;

				case SchemaType.Map:
					ProcessMap((MapField)f, (IReadOnlyCollection<Row>?)row?[cellIndex] ?? Array.Empty<Row>(), level + 1, indexes);
					break;

				case SchemaType.Struct:
					ProcessRow(((StructField)f).Fields, (Row?)row?[cellIndex], level + 1, indexes);
					break;

				case SchemaType.List:
					ProcessList((ListField)f, row?[cellIndex] ?? Array.Empty<object>(), level + 1, indexes);
					break;

				default:
					throw new NotImplementedException();
			}

			cellIndex++;
		}
	}

	private void ProcessMap(MapField mapField, IReadOnlyCollection<Row> mapRows, int level, LevelIndex[] indexes)
	{
		var fields = new Field[] { mapField.Key, mapField.Value };

		var keyCell = mapRows.Select(r => r[0]).ToList();
		var valueCell = mapRows.Select(r => r[1]).ToList();
		var row = new Row(keyCell, valueCell);

		ProcessRow(fields, row, level, indexes);
	}

	private void ProcessList(ListField listField, object cellValue, int level, LevelIndex[] indexes)
	{
		Field f = listField.Item;

		switch (f.SchemaType)
		{
			case SchemaType.Data:
				//list has a special case for simple elements where they are not wrapped in rows
				ProcessDataValue(f, cellValue, indexes);
				break;
			case SchemaType.Struct:
				ProcessRows(((StructField)f).Fields, (IReadOnlyCollection<Row>)cellValue, level, indexes);
				break;
			default:
				throw new NotSupportedException();
		}
	}

	private void ProcessDataValue(Field f, object? value, LevelIndex[] indexes)
	{
		GetAppender(f).Add(value, indexes);
	}

	private DataColumnAppender GetAppender(Field f)
	{
		//prepare value appender
		if (!pathToDataColumn.TryGetValue(f.Path, out DataColumnAppender? appender))
		{
			appender = new DataColumnAppender((DataField)f);
			pathToDataColumn[f.Path] = appender;
			flattenedFieldList.Add(f);
		}

		return appender;
	}
}

class DataColumnAppender
{
	private readonly DataField _dataField;
	private readonly List<object?> _values = new List<object?>();
	private readonly List<int> _rls = new List<int>();
	private readonly bool _isRepeated;
	private LevelIndex[]? _lastIndexes;

	public DataColumnAppender(DataField dataField)
	{
		_dataField = dataField;
		_isRepeated = dataField.MaxRepetitionLevel > 0;
	}

	public void Clear()
	{
		_values.Clear();
		_rls.Clear();
	}

	public void Add(object? value, LevelIndex[] indexes)
	{
		if (_isRepeated)
		{
			int rl = GetRepetitionLevel(indexes, _lastIndexes);

			if (!(value is string) && value is IEnumerable valueItems)
			{
				int count = 0;
				foreach (object valueItem in (IEnumerable)value)
				{
					_values.Add(valueItem);
					_rls.Add(rl);
					rl = _dataField.MaxRepetitionLevel;
					count += 1;
				}

				if (count == 0)
				{
					//handle empty collections
					_values.Add(null);
					_rls.Add(0);
				}
			}
			else
			{
				_values.Add(value);
				_rls.Add(rl);
			}

			_lastIndexes = indexes;
		}
		else
		{
			//non-repeated fields can only appear on the first level and have no repetition levels (obviously)
			_values.Add(value);
		}

	}

	public DataColumn ToDataColumn()
	{
		Array data = Array.CreateInstance(_dataField.ClrNullableIfHasNullsType, _values.Count);

		for (int i = 0; i < _values.Count; i++)
		{
			data.SetValue(_values[i], i);
		}

		return new DataColumn(_dataField, data, _isRepeated ? _rls.ToArray() : null);
	}

	public override string ToString() => _dataField.ToString();

	private static int GetRepetitionLevel(LevelIndex[] currentIndexes, LevelIndex[]? lastIndexes)
	{
		for (int i = 0; i < (lastIndexes?.Length ?? 0); i++)
		{
			if (currentIndexes[i].Index != lastIndexes![i].Index)
				return lastIndexes[i].Level;
		}

		return 0;
	}
}

struct LevelIndex
{
	public LevelIndex(
	  int level,
	  int index)
	{
		Level = level;
		Index = index;
	}

	public int Level;
	public int Index;

	public override string ToString()
	{
		return $"Level: {Level}; Index: {Index}";
	}

	public override bool Equals(object? obj)
	{
		if (obj is LevelIndex li &&
		   li.Index == Index &&
		   li.Level == Level)
			return true;

		return false;
	}

	public override int GetHashCode()
	{
		return Level.GetHashCode() * 17 + Index.GetHashCode();
	}
}

