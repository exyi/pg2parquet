using System.Runtime.CompilerServices;
using Npgsql;
using Parquet;
using Parquet.Data;

namespace Pg2parquet;

public interface IColumnWriter
{
	void Init(int batchSize, Field field);
	void Reset();
	IEnumerable<DataColumn> GetColumns();
	void WriteValue(
		NpgsqlBinaryExporter reader
	);
}
