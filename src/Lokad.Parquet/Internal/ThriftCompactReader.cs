using System.Text;

namespace Lokad.Parquet.Internal;

internal sealed class ThriftTruncatedException(long byteOffset) : Exception
{
    public long ByteOffset { get; } = byteOffset;
}

internal enum CompactType : byte
{
    Stop = 0,
    BooleanTrue = 1,
    BooleanFalse = 2,
    Byte = 3,
    Int16 = 4,
    Int32 = 5,
    Int64 = 6,
    Double = 7,
    Binary = 8,
    List = 9,
    Set = 10,
    Map = 11,
    Struct = 12,
}

internal enum CompactStructContext
{
    Metadata,
    PageHeader,
}

internal readonly record struct CompactField(short Id, CompactType Type);
internal readonly record struct CompactCollection(CompactType ElementType, int Count);

internal ref struct ThriftCompactReader
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private readonly ReadOnlySpan<byte> _input;
    private readonly long _baseOffset;
    private readonly int _maximumContainerElements;
    private readonly int _maximumDepth;
    private readonly int _maximumStringBytes;
    private readonly CancellationToken _cancellationToken;
    private int _stringBytes;
    private int _position;

    public ThriftCompactReader(ReadOnlySpan<byte> input, long baseOffset, ParquetReaderOptions options, CancellationToken cancellationToken)
    {
        _input = input;
        _baseOffset = baseOffset;
        _cancellationToken = cancellationToken;
        _maximumContainerElements = options.MaximumThriftContainerElements;
        _maximumDepth = options.MaximumThriftDepth;
        _maximumStringBytes = options.MaximumMetadataStringBytes;
        _stringBytes = 0;
        _position = 0;
    }

    public int Position => _position;
    public bool IsAtEnd => _position == _input.Length;

    // Observes cancellation for long wire loops at a bounded interval. Loop bodies
    // call this with their element ordinal; the token belongs to the enclosing parse.
    public void ObserveCancellation(int index)
    {
        if ((index & 1023) == 0)
            _cancellationToken.ThrowIfCancellationRequested();
    }
    public long AbsoluteOffset => checked(_baseOffset + _position);

    public void MarkKnownField(ref ulong seen, int fieldId, CompactStructContext context)
    {
        if (fieldId is <= 0 or >= 64)
            return;
        var bit = 1UL << fieldId;
        if ((seen & bit) != 0)
        {
            throw Format(context switch
            {
                CompactStructContext.Metadata => "A Thrift struct contains a duplicate field.",
                CompactStructContext.PageHeader => "A page-header struct contains a duplicate field.",
                _ => throw new ArgumentOutOfRangeException(nameof(context)),
            });
        }
        seen |= bit;
    }

    public CompactField ReadField(ref short previousFieldId)
    {
        var header = ReadRawByte();
        var type = ParseType((byte)(header & 0x0F));
        if (type == CompactType.Stop)
            return new CompactField(0, type);

        var delta = header >> 4;
        short fieldId;
        if (delta == 0)
        {
            var value = ZigZag32(ReadVarUInt32());
            if (value < short.MinValue || value > short.MaxValue)
                ThrowFormat("Thrift i16 value is out of range.");
            fieldId = (short)value;
        }
        else
        {
            var candidate = (int)previousFieldId + delta;
            if (candidate > short.MaxValue)
                ThrowFormat("Thrift field identifier overflow.");
            fieldId = (short)candidate;
        }

        if (fieldId <= 0)
            ThrowFormat("Thrift field identifiers must be positive.");
        previousFieldId = fieldId;
        return new CompactField(fieldId, type);
    }

    public bool ReadBoolean(CompactType type) => type switch
    {
        CompactType.BooleanTrue => true,
        CompactType.BooleanFalse => false,
        _ => throw Format("Expected a compact boolean field."),
    };

    public sbyte ReadSByte()
    {
        var value = ReadRawByte();
        return unchecked((sbyte)value);
    }

    public int ReadInt32() => ZigZag32(ReadVarUInt32());

    public long ReadInt64()
    {
        var value = ReadVarUInt64();
        return unchecked((long)((value >> 1) ^ (ulong)-(long)(value & 1)));
    }

    public byte[] ReadBinary()
    {
        var length = ReadLength();
        return ReadSpan(length).ToArray();
    }

    public string ReadString()
    {
        var length = ReadLength();
        if (length > _maximumStringBytes - _stringBytes)
            throw new ParquetLimitExceededException("Metadata strings exceed the configured byte limit.", ParquetErrorLocation.AtOffset(AbsoluteOffset));
        _stringBytes += length;
        var bytes = ReadSpan(length);
        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException exception)
        {
            throw new ParquetFormatException("Metadata contains malformed UTF-8.", exception, ParquetErrorLocation.AtOffset(AbsoluteOffset - length));
        }
    }

    public CompactCollection ReadCollection()
    {
        var header = ReadRawByte();
        var count = header >> 4;
        if (count == 15)
        {
            count = ReadContainerCount();
        }
        else if (count > _maximumContainerElements)
        {
            throw new ParquetLimitExceededException("A Thrift container exceeds the configured element limit.", ParquetErrorLocation.AtOffset(AbsoluteOffset));
        }
        var type = ParseType((byte)(header & 0x0F));
        if (type == CompactType.Stop)
            ThrowFormat("A collection element cannot use the STOP type.");
        return new CompactCollection(type, count);
    }

    // Thrift depth model. A struct value nested through fields and containers inside
    // a depth-D struct is itself at depth D + 1; the outermost struct is at depth 1.
    // Containers and scalar members are transparent to known parsing: only struct
    // entries assert, with their own depth, and list calls assert the enclosing depth.
    // Skip entry points take the enclosing struct depth and add one per entered value,
    // so skipped subtrees stay within one level of known subtrees under the single
    // shared depth cap.
    public void RequireDepth(int depth)
    {
        if (depth > _maximumDepth)
            throw new ParquetLimitExceededException("Thrift nesting exceeds the configured depth limit.", ParquetErrorLocation.AtOffset(AbsoluteOffset));
    }

    // Requires that a declared element count can still be backed by the remaining
    // input before the caller allocates a count-sized array. Every compact element
    // occupies at least one byte, so a larger count is malformed or truncated.
    public void RequireCountFitsRemaining(int count)
    {
        if (count > _input.Length - _position)
            throw Format("A Thrift container declares more elements than the remaining input.");
    }

    public void RequireType(CompactField field, CompactType expected)
    {
        if (field.Type != expected)
            ThrowFormat($"Thrift field {field.Id} has an unexpected wire type.");
    }

    public void SkipField(CompactField field, int depth) =>
        SkipValue(field.Type, depth, CompactBooleanEncoding.FieldHeader);

    public void SkipValue(CompactType type, int depth, CompactBooleanEncoding booleanEncoding)
    {
        if (depth > _maximumDepth)
            throw new ParquetLimitExceededException("Thrift nesting exceeds the configured depth limit.", ParquetErrorLocation.AtOffset(AbsoluteOffset));

        switch (type)
        {
            case CompactType.BooleanTrue:
            case CompactType.BooleanFalse:
                if (booleanEncoding == CompactBooleanEncoding.CollectionValue)
                {
                    var value = ReadRawByte();
                    if (value is not (byte)CompactType.BooleanTrue and not (byte)CompactType.BooleanFalse)
                        ThrowFormat("Invalid compact boolean value.");
                }
                return;
            case CompactType.Byte:
                ReadRawByte();
                return;
            case CompactType.Int16:
            case CompactType.Int32:
                ReadVarUInt32();
                return;
            case CompactType.Int64:
                ReadVarUInt64();
                return;
            case CompactType.Double:
                ReadSpan(sizeof(long));
                return;
            case CompactType.Binary:
                ReadSpan(ReadLength());
                return;
            case CompactType.List:
            case CompactType.Set:
                {
                    var collection = ReadCollection();
                    for (var i = 0; i < collection.Count; i++)
                    {
                        ObserveCancellation(i);
                        SkipValue(collection.ElementType, depth + 1, CompactBooleanEncoding.CollectionValue);
                    }
                    return;
                }
            case CompactType.Map:
                {
                    var count = ReadContainerCount();
                    if (count == 0)
                        return;
                    var types = ReadRawByte();
                    var keyType = ParseType((byte)(types >> 4));
                    var valueType = ParseType((byte)(types & 0x0F));
                    if (keyType == CompactType.Stop || valueType == CompactType.Stop)
                        ThrowFormat("A map element cannot use the STOP type.");
                    for (var i = 0; i < count; i++)
                    {
                        ObserveCancellation(i);
                        SkipValue(keyType, depth + 1, CompactBooleanEncoding.CollectionValue);
                        SkipValue(valueType, depth + 1, CompactBooleanEncoding.CollectionValue);
                    }
                    return;
                }
            case CompactType.Struct:
                {
                    short previous = 0;
                    var skippedFields = 0;
                    while (true)
                    {
                        var field = ReadField(ref previous);
                        if (field.Type == CompactType.Stop)
                            return;
                        ObserveCancellation(skippedFields++);
                        SkipField(field, depth + 1);
                    }
                }
            default:
                ThrowFormat("Invalid compact value type.");
                return;
        }
    }

    public ParquetFormatException Format(string message) => new(message, ParquetErrorLocation.AtOffset(AbsoluteOffset));

    private int ReadLength()
    {
        var length = ReadVarUInt32();
        if (length > int.MaxValue)
            throw new ParquetLimitExceededException("A Thrift binary value exceeds the runtime buffer limit.", ParquetErrorLocation.AtOffset(AbsoluteOffset));
        return (int)length;
    }

    private int ReadContainerCount()
    {
        var count = ReadVarUInt32();
        if (count > (uint)_maximumContainerElements)
            throw new ParquetLimitExceededException("A Thrift container exceeds the configured element limit.", ParquetErrorLocation.AtOffset(AbsoluteOffset));
        return (int)count;
    }

    private byte ReadRawByte()
    {
        if ((uint)_position >= (uint)_input.Length)
            throw new ThriftTruncatedException(AbsoluteOffset);
        return _input[_position++];
    }

    private ReadOnlySpan<byte> ReadSpan(int length)
    {
        if (length < 0 || length > _input.Length - _position)
            throw new ThriftTruncatedException(AbsoluteOffset);
        var result = _input.Slice(_position, length);
        _position += length;
        return result;
    }

    private uint ReadVarUInt32()
    {
        uint value = 0;
        for (var shift = 0; shift < 35; shift += 7)
        {
            var current = ReadRawByte();
            if (shift == 28 && (current & 0xF0) != 0)
                ThrowFormat("Thrift varint overflow.");
            value |= (uint)(current & 0x7F) << shift;
            if ((current & 0x80) == 0)
                return value;
        }
        ThrowFormat("Unterminated Thrift varint.");
        return 0;
    }

    private ulong ReadVarUInt64()
    {
        ulong value = 0;
        for (var shift = 0; shift < 70; shift += 7)
        {
            var current = ReadRawByte();
            if (shift == 63 && (current & 0xFE) != 0)
                ThrowFormat("Thrift varint overflow.");
            value |= (ulong)(current & 0x7F) << shift;
            if ((current & 0x80) == 0)
                return value;
        }
        ThrowFormat("Unterminated Thrift varint.");
        return 0;
    }

    private static int ZigZag32(uint value) => unchecked((int)((value >> 1) ^ (uint)-(int)(value & 1)));

    private CompactType ParseType(byte value)
    {
        if (value > (byte)CompactType.Struct)
            ThrowFormat("Unknown Thrift Compact type code.");
        return (CompactType)value;
    }

    private void ThrowFormat(string message) => throw Format(message);
}

internal enum CompactBooleanEncoding { FieldHeader, CollectionValue }
