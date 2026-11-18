namespace Lokad.Parquet.Internal;

// The Parquet writer emits only increasing field IDs with deltas below 16.
internal ref struct ThriftCompactWriter
{
    private readonly Span<byte> _buffer;
    private int _length;

    public ThriftCompactWriter(Span<byte> buffer) => _buffer = buffer;

    public readonly int Length => _length;

    public void Stop() => _buffer[_length++] = (byte)CompactType.Stop;

    public void StructField(ref int previous, int id) => Field(ref previous, id, CompactType.Struct);

    public void Int32Field(ref int previous, int id, int value)
    {
        Field(ref previous, id, CompactType.Int32);
        Int32(value);
    }

    public void Int64Field(ref int previous, int id, long value)
    {
        Field(ref previous, id, CompactType.Int64);
        VarUInt(unchecked((ulong)((value << 1) ^ (value >> 63))));
    }

    public void BinaryField(ref int previous, int id, ReadOnlySpan<byte> value)
    {
        Field(ref previous, id, CompactType.Binary);
        Binary(value);
    }

    public void ListField(ref int previous, int id, CompactType type, int count)
    {
        Field(ref previous, id, CompactType.List);
        _buffer[_length++] = (byte)((Math.Min(count, 15) << 4) | (int)type);
        if (count >= 15)
            VarUInt((uint)count);
    }

    public void Int32(int value) => VarUInt(unchecked((uint)((value << 1) ^ (value >> 31))));

    public void Binary(ReadOnlySpan<byte> value)
    {
        VarUInt((uint)value.Length);
        value.CopyTo(_buffer[_length..]);
        _length += value.Length;
    }

    private void Field(ref int previous, int id, CompactType type)
    {
        System.Diagnostics.Debug.Assert(id - previous is > 0 and < 16);
        _buffer[_length++] = (byte)(((id - previous) << 4) | (int)type);
        previous = id;
    }

    private void VarUInt(ulong value)
    {
        while (value >= 128)
        {
            _buffer[_length++] = (byte)(value | 128);
            value >>= 7;
        }
        _buffer[_length++] = (byte)value;
    }
}
