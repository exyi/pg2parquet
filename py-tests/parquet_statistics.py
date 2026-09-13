"""Check stored row-group statistics against data read without filter pruning."""

import math
import struct
import warnings

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def _leaves(array, nested=False):
    """Yield physical leaves in Parquet order, excluding absent parents."""
    if pa.types.is_struct(array.type):
        present = pc.filter(array, pc.is_valid(array))
        for child in present.flatten():
            yield from _leaves(child, True)
    elif pa.types.is_map(array.type):
        present = pc.filter(array, pc.is_valid(array))
        yield from _leaves(present.values, True)
    elif (pa.types.is_list(array.type) or pa.types.is_large_list(array.type)
          or pa.types.is_fixed_size_list(array.type)):
        yield from _leaves(pc.list_flatten(array), True)
    else:
        yield array, nested


def _comparable_values(array, stats):
    """Choose a representation shared by the values and statistics."""
    if pa.types.is_dictionary(array.type):
        array = array.dictionary_decode()
    if isinstance(array, pa.ExtensionArray):
        array = array.storage
    dtype = array.type
    if pa.types.is_temporal(dtype):
        # Avoid Python datetime overflow for PostgreSQL infinity and wide dates.
        width = dtype.bit_width
        array = array.cast(pa.int32() if width == 32 else pa.int64())
        bounds = stats.min_raw, stats.max_raw
    elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        array = array.cast(pa.binary() if pa.types.is_string(dtype) else pa.large_binary())
        bounds = stats.min_raw, stats.max_raw
    elif pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_fixed_size_binary(dtype):
        bounds = stats.min_raw, stats.max_raw
    elif pa.types.is_float16(dtype):
        def decode(value):
            return struct.unpack('<e', value)[0] if isinstance(value, bytes) else value
        bounds = decode(stats.min_raw), decode(stats.max_raw)
    else:
        bounds = stats.min, stats.max
    values = [v for v in array.to_pylist()
              if v is not None and not (isinstance(v, float) and math.isnan(v))]
    return values, bounds


def validate_statistics(path):
    """Validate every readable leaf's existing statistics in every row group.

    Binary/string bounds may be truncated, so require enclosure rather than
    equality. Other bounds must equal the actual extrema. Nested null counts
    include repetition-level placeholders, so only check flat null counts.
    Files whose types PyArrow cannot decode are explicitly reported as skipped.
    """
    try:
        parquet = pq.ParquetFile(path)
    except pa.ArrowNotImplementedError as error:
        warnings.warn(f'Statistics validation skipped for {path}: {error}')
        return
    except pa.ArrowInvalid as error:
        # Parquet supports precisions beyond Arrow's Decimal256 limit.
        if not ('precision' in str(error).lower() and 'range' in str(error).lower()):
            raise
        warnings.warn(f'Statistics validation skipped for {path}: {error}')
        return
    for group_index in range(parquet.num_row_groups):
        try:
            table = parquet.read_row_group(group_index)
        except pa.ArrowNotImplementedError as error:
            warnings.warn(f'Statistics validation skipped for {path}, group {group_index}: {error}')
            continue
        group = parquet.metadata.row_group(group_index)
        leaves = [leaf for column in table.columns
                  for leaf in _leaves(column.combine_chunks())]
        assert len(leaves) == group.num_columns, (path, group_index, 'leaf count mismatch')
        for index, (array, nested) in enumerate(leaves):
            column = group.column(index)
            label = f'{path}, row group {group_index}, column {column.path_in_schema}'
            stats = column.statistics
            if stats is None:
                continue
            if not nested and stats.has_null_count:
                assert stats.null_count == array.null_count, f'{label}: incorrect null count'
            if not stats.has_min_max:
                continue
            values, (lower, upper) = _comparable_values(array, stats)
            assert values, f'{label}: min/max present without comparable values'
            actual_min, actual_max = min(values), max(values)
            if isinstance(actual_min, bytes):
                valid = lower <= actual_min <= actual_max <= upper
            else:
                valid = lower == actual_min and upper == actual_max
            assert valid, (f'{label}: stored bounds [{lower!r}, {upper!r}], '
                           f'actual bounds [{actual_min!r}, {actual_max!r}]')
