using System.Numerics;
using System.Runtime.CompilerServices;

namespace Pg2parquet;

static class MemUtils
{
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Append<T>(ref T[] array, int index, T value)
	{
		Append<T>(ref array, ref index, value);
	}
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Append<T>(ref T[] array, ref int index, T value)
	{
		if (index >= array.Length)
		{
			Resize(ref array, 1);
		}

		array[index] = value;
		index++;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void AppendRange<T>(ref T[] array, int index, Span<T> values) =>
		AppendRange<T>(ref array, ref index, values);
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void AppendRange<T>(ref T[] array, ref int index, Span<T> values)
	{
		if (index + values.Length > array.Length)
		{
			Resize(ref array, values.Length);
		}

		values.CopyTo(array.AsSpan(index));
		index += values.Length;
	}

	public static void AppendMultiple<T>(ref T[] array, int index, T value, int repeat) =>
		AppendMultiple<T>(ref array, ref index, value, repeat);
	public static void AppendMultiple<T>(ref T[] array, ref int index, T value, int repeat)
	{
		if (index + repeat > array.Length)
		{
			Resize(ref array, repeat);
		}

		for (int i = 0; i < repeat; i++)
		{
			array[index + i] = value;
		}
		index += repeat;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void EnsureSize<T>(ref T[] array, int size)
	{
		var x = size - array.Length;
		if (x > 0)
		{
			Resize(ref array, x);
		}
	}


	[MethodImpl(MethodImplOptions.NoInlining)]
	static void Resize<T>(ref T[] array, int increase)
	{
		var newArray = new T[Math.Max(array.Length * 2, BitOperations.RoundUpToPowerOf2(unchecked((uint)(array.Length + increase))))];
		Array.Copy(array, newArray, array.Length);
		array = newArray;
	}

}
