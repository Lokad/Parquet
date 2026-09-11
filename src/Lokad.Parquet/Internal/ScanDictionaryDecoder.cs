using System.Buffers.Binary;

namespace Lokad.Parquet.Internal;

// Plain page decoder signature shared by the scan cursor and the dictionary
// decoder below.
internal delegate void PlainPageDecode<T>(ReadOnlySpan<byte> source, Span<T> destination, CancellationToken cancellationToken)
    where T : unmanaged;

// Dictionary page decoder for one column scan. Owns the row-group dictionary
// store across data pages: primitive entries in a bound lease, variable-width
// entries in explicit owners, plus the entry count used for index validation.
// The cursor drives page progression and keeps page outputs; this component
// receives options, budget, page sinks, error location and cancellation
// explicitly and captures nothing. Decoding matches the cursor locals it was
// extracted from; only the organization changed.
internal sealed class ScanDictionaryDecoder : IDisposable
{
    private readonly ParquetColumn _column;
    private readonly IColumnValueCacheProvider _cacheProvider;
    private PooledValueLease _values;
    private PooledArrayOwner<int>? _binaryOffsets;
    private PooledArrayOwner<byte>? _binaryPayload;
    private PooledArrayOwner<byte>? _fixedPayload;
    private int _fixedWidth;
    private int _count;

    internal ScanDictionaryDecoder(ParquetColumn column, IColumnValueCacheProvider cacheProvider)
    {
        _column = column;
        _cacheProvider = cacheProvider;
    }

    internal bool HasDictionary => _values.HasValues || _binaryOffsets is not null || _fixedPayload is not null;

    public void DecodePage(
        ReadOnlySpan<byte> payload,
        int valueCount,
        ParquetPhysicalType physicalType,
        ParquetReaderOptions options,
        ParquetScanMemoryBudget budget,
        ParquetErrorLocation location,
        CancellationToken cancellationToken)
    {
        if (physicalType == ParquetPhysicalType.ByteArray)
        {
            SetBinaryDictionary(payload, valueCount);
            return;
        }
        if (physicalType == ParquetPhysicalType.FixedLengthByteArray)
        {
            SetFixedDictionary(payload, valueCount);
            return;
        }

        var expectedByteCount = PlainDecoder.GetPlainByteCount(physicalType, valueCount, location);
        if (payload.Length != expectedByteCount)
            throw new ParquetFormatException("A PLAIN dictionary payload length does not match its entry count.", location);

        switch (physicalType)
        {
            case ParquetPhysicalType.Boolean:
                SetDictionary<bool>(payload, valueCount, PlainDecoder.DecodeBoolean);
                break;
            case ParquetPhysicalType.Int32:
                SetDictionary<int>(payload, valueCount, PlainDecoder.DecodeInt32);
                break;
            case ParquetPhysicalType.Int64:
                SetDictionary<long>(payload, valueCount, PlainDecoder.DecodeInt64);
                break;
            case ParquetPhysicalType.Float:
                SetDictionary<float>(payload, valueCount, PlainDecoder.DecodeFloat);
                break;
            case ParquetPhysicalType.Double:
                SetDictionary<double>(payload, valueCount, PlainDecoder.DecodeDouble);
                break;
            default:
                throw new ParquetUnsupportedFeatureException("The dictionary physical type is unsupported.", location);

        }

        void SetBinaryDictionary(ReadOnlySpan<byte> payload, int valueCount)
        {
            // Serialized bytes were checked against MaximumDictionaryBytes at the page header; decoded layout is checked after lengths are known below.
            // Compact decoded layout is offsets plus payload bytes without length prefixes.
            PooledArrayOwner<int>? offsets = PooledArrayOwner<int>.Rent(checked(valueCount + 1), budget);
            PooledArrayOwner<byte>? values = null;
            try
            {
                var inputOffset = 0;
                var aggregateLength = 0;
                offsets.Memory.Span[0] = 0;
                for (var index = 0; index < valueCount; index++)
                {
                    if ((index & 1023) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    if (payload.Length - inputOffset < sizeof(int))
                        throw new ParquetFormatException("A PLAIN BYTE_ARRAY dictionary length is truncated.", location);
                    var length = BinaryPrimitives.ReadInt32LittleEndian(payload[inputOffset..]);
                    inputOffset += sizeof(int);
                    if (length < 0 || length > payload.Length - inputOffset)
                        throw new ParquetFormatException("A PLAIN BYTE_ARRAY dictionary length is invalid.", location);
                    if (length > options.MaximumBinaryValueBytes)
                        throw new ParquetLimitExceededException("A BYTE_ARRAY dictionary value exceeds the configured byte limit.", location);

                    aggregateLength += length;
                    inputOffset += length;
                    offsets.Memory.Span[index + 1] = aggregateLength;
                }
                if (inputOffset != payload.Length)
                    throw new ParquetFormatException("A PLAIN BYTE_ARRAY dictionary has trailing bytes.", location);
                var decodedBytes = checked((valueCount + 1L) * sizeof(int) + (long)aggregateLength);
                if (decodedBytes > options.MaximumDictionaryBytes)
                    throw new ParquetLimitExceededException("A decoded BYTE_ARRAY dictionary exceeds its configured byte limit.", location);


                values = PooledArrayOwner<byte>.Rent(aggregateLength, budget);
                inputOffset = 0;
                var outputOffset = 0;
                for (var index = 0; index < valueCount; index++)
                {
                    if ((index & 1023) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    var length = BinaryPrimitives.ReadInt32LittleEndian(payload[inputOffset..]);
                    inputOffset += sizeof(int);
                    payload.Slice(inputOffset, length).CopyTo(values.Memory.Span[outputOffset..]);
                    inputOffset += length;
                    outputOffset += length;
                }

                _binaryOffsets = offsets;
                _binaryPayload = values;
                _count = valueCount;
                offsets = null;
                values = null;
            }
            catch
            {
                // Best-effort rollback preserves the primary page error.
                try { offsets?.Dispose(); } catch (Exception) { }
                try { values?.Dispose(); } catch (Exception) { }
                throw;
            }
        }

        void SetFixedDictionary(ReadOnlySpan<byte> payload, int valueCount)
        {
            var width = _column.SchemaElement.TypeLength ?? 0;
            if (width <= 0)
                throw new ParquetFormatException("A FIXED_LEN_BYTE_ARRAY column has an invalid width.", location);
            if (width > options.MaximumBinaryValueBytes)
                throw new ParquetLimitExceededException("A FIXED_LEN_BYTE_ARRAY dictionary value exceeds the configured byte limit.", location);
            if (valueCount > payload.Length / width || valueCount * width != payload.Length)
                throw new ParquetFormatException("A PLAIN FIXED_LEN_BYTE_ARRAY dictionary payload length is inconsistent.", location);
            PooledArrayOwner<byte>? values = PooledArrayOwner<byte>.Rent(payload.Length, budget);
            try
            {
                payload.CopyTo(values.Memory.Span);
                _fixedPayload = values;
                values = null;
                _fixedWidth = width;
                _count = valueCount;
            }
            catch
            {
                // Best-effort rollback preserves the primary dictionary error.
                try { values?.Dispose(); } catch (Exception) { }
                throw;
            }
        }

        void SetDictionary<T>(ReadOnlySpan<byte> payload, int valueCount, PlainPageDecode<T> decoder)
        where T : unmanaged
        {
            long decodedBytes = typeof(T) == typeof(bool) ? valueCount : typeof(T) == typeof(int) || typeof(T) == typeof(float) ? checked((long)valueCount * sizeof(int)) : checked((long)valueCount * sizeof(long));
            if (decodedBytes > options.MaximumDictionaryBytes)
                throw new ParquetLimitExceededException("A decoded dictionary exceeds its configured byte limit.", location);

            PooledArrayOwner<T>? values = PooledArrayOwner<T>.Rent(valueCount, budget);
            try
            {
                decoder(payload, values.Memory.Span, cancellationToken);
                _values.Set(values);
                _count = valueCount;
                values = null;
            }
            catch
            {
                // Best-effort rollback preserves the primary page error.
                try { values?.Dispose(); } catch (Exception) { }
                throw;
            }
        }

    }

    public void ExpandDataPage(
        ValidatedPageHeader header,
        ReadOnlySpan<byte> payload,
        int rowCount,
        ParquetPhysicalType physicalType,
        ParquetReaderOptions options,
        ParquetScanMemoryBudget budget,
        ref PooledValueLease pageValues,
        ref PageValidityState pageValidity,
        ref PooledArrayOwner<int>? pageBinaryOffsets,
        ref PooledArrayOwner<byte>? pageBinaryPayload,
        ref PooledArrayOwner<byte>? pageFixedPayload,
        ref int pageFixedWidth,
        ParquetErrorLocation location,
        CancellationToken cancellationToken)
    {
        // Optional definition levels decode straight into a validity bitmap,
        // reusing the item-18 primitive layout: no int level array, no
        // validity rescan. The bitmap doubles as the page validity on success
        // and is released by the levels failure policy on any error.
        PooledArrayOwner<byte>? levelsBitmap = null;
        Exception? levelsFailure = null;
        try
        {
            var optional = _column.SchemaElement.Repetition == ParquetRepetition.Optional;
            int physicalOffset;
            int physicalCount;
            if (!optional)
            {
                if (header.PageType == ValidatedPageType.DataV2)
                    DefinitionLevelCodec.ValidateRequiredV2(header.DataV2, rowCount, location);
                physicalOffset = 0;
                physicalCount = rowCount;
            }
            else
            {
                int levelOffset;
                int levelByteCount;
                int? expectedNullCount;
                if (header.PageType == ValidatedPageType.DataV1)
                {
                    if (header.DataV1.DefinitionEncodingCode != (int)ParquetEncoding.RunLength)
                        throw new ParquetUnsupportedFeatureException("Optional definition levels require V1 RLE/bit-packed hybrid encoding.", location);
                    var section = DefinitionLevelCodec.SplitOptionalV1Section(payload, location);
                    levelOffset = section.LevelOffset;
                    levelByteCount = section.LevelByteCount;
                    physicalOffset = section.PhysicalOffset;
                    expectedNullCount = null;
                }
                else
                {
                    if (header.PageType != ValidatedPageType.DataV2)
                        throw new InvalidOperationException("A validated V2 page has no V2 header.");
                    var v2 = header.DataV2;
                    if (v2.RowCount != rowCount || v2.RepetitionLevelsByteLength != 0)
                        throw new ParquetFormatException("A flat optional V2 page has inconsistent row or repetition fields.", location);
                    levelOffset = 0;
                    levelByteCount = v2.DefinitionLevelsByteLength;
                    physicalOffset = levelByteCount;
                    expectedNullCount = v2.NullCount;
                }
                if (levelOffset < 0 || levelByteCount < 0 || physicalOffset < 0 ||
                    levelByteCount > payload.Length - levelOffset ||
                    physicalOffset > payload.Length)
                    throw new ParquetFormatException("An optional page has invalid level or value boundaries.", location);
                levelsBitmap = PooledArrayOwner<byte>.Rent(checked((rowCount + 7) / 8), budget);
                int validCount;
                int consumed;
                try
                {
                    consumed = RleBitPackedHybridDecoder.DecodeBitWidthOneToBitmap(
                        payload.Slice(levelOffset, levelByteCount),
                        rowCount,
                        levelsBitmap.Memory.Span,
                        cancellationToken,
                        out validCount);
                }
                catch (ParquetFormatException exception)
                {
                    throw new ParquetFormatException(exception.Message, exception, new ParquetErrorLocation(exception.ByteOffset ?? location.ByteOffset, location.RowGroupOrdinal, location.ColumnOrdinal, location.PageOrdinal));
                }
                if (consumed != levelByteCount)
                    throw new ParquetFormatException("An optional page has trailing definition-level bytes.", location);
                if (expectedNullCount is int nullCount && rowCount - validCount != nullCount)
                    throw new ParquetFormatException("A V2 page null count does not match its definition levels.", location);
                physicalCount = validCount;
                if (validCount == rowCount)
                {
                    // All-valid pages carry no bitmap; the tight expansions
                    // below overwrite every output slot.
                    levelsBitmap.Dispose();
                    levelsBitmap = null;
                }
            }

            if (physicalOffset >= payload.Length)
                throw new ParquetFormatException("A dictionary data page is missing its index bit width.", location);
            var bitWidth = payload[physicalOffset++];
            PooledArrayOwner<int>? indices = null;
            Exception? indicesFailure = null;
            try
            {
                indices = PooledArrayOwner<int>.Rent(physicalCount, budget);
                var encodedIndices = payload[physicalOffset..];
                int indexConsumed;
                try
                {
                    indexConsumed = RleBitPackedHybridDecoder.Decode(
                        encodedIndices,
                        bitWidth,
                        indices.Memory.Span,
                        cancellationToken);
                }
                catch (ParquetFormatException exception)
                {
                    throw new ParquetFormatException(exception.Message, exception, new ParquetErrorLocation(exception.ByteOffset ?? location.ByteOffset, location.RowGroupOrdinal, location.ColumnOrdinal, location.PageOrdinal));
                }
                if (indexConsumed != encodedIndices.Length)
                    throw new ParquetFormatException("A dictionary data page has trailing index bytes.", location);

                // Index bounds fuse into the expansions below: each consumed
                // index is range-checked as it is read, so no standalone
                // validation pass remains. An unsupported physical type still
                // reports unsupported before any index bounds check.
                var validityBits = levelsBitmap is null
                    ? ReadOnlySpan<byte>.Empty
                    : levelsBitmap.Memory.Span;
                switch (physicalType)
                {
                    case ParquetPhysicalType.Boolean:
                        ExpandValues<bool>(indices.Memory.Span, validityBits, optional, rowCount, ref pageValues, location, cancellationToken);
                        break;
                    case ParquetPhysicalType.Int32:
                        ExpandValues<int>(indices.Memory.Span, validityBits, optional, rowCount, ref pageValues, location, cancellationToken);
                        break;
                    case ParquetPhysicalType.Int64:
                        ExpandValues<long>(indices.Memory.Span, validityBits, optional, rowCount, ref pageValues, location, cancellationToken);
                        break;
                    case ParquetPhysicalType.Float:
                        ExpandValues<float>(indices.Memory.Span, validityBits, optional, rowCount, ref pageValues, location, cancellationToken);
                        break;
                    case ParquetPhysicalType.Double:
                        ExpandValues<double>(indices.Memory.Span, validityBits, optional, rowCount, ref pageValues, location, cancellationToken);
                        break;
                    case ParquetPhysicalType.ByteArray:
                        ExpandBinaryDictionary(indices.Memory.Span, validityBits, optional, rowCount, options, budget, ref pageBinaryOffsets, ref pageBinaryPayload, location, cancellationToken);
                        break;
                    case ParquetPhysicalType.FixedLengthByteArray:
                        ExpandFixedDictionary(indices.Memory.Span, validityBits, optional, rowCount, options, budget, ref pageFixedPayload, ref pageFixedWidth, location, cancellationToken);
                        break;
                    default:
                        throw new ParquetUnsupportedFeatureException("The dictionary physical type is unsupported.", location);

                }
                pageValidity.SetFromNullable(levelsBitmap);
                levelsBitmap = null;
            }
            catch (Exception exception)
            {
                indicesFailure = exception;
            }
            try { indices?.Dispose(); } catch (Exception exception) when (indicesFailure is null) { indicesFailure = exception; } catch (Exception) { }
            if (indicesFailure is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(indicesFailure).Throw();
        }
        catch (Exception exception)
        {
            levelsFailure = exception;
        }
        try { levelsBitmap?.Dispose(); } catch (Exception exception) when (levelsFailure is null) { levelsFailure = exception; } catch (Exception) { }
        if (levelsFailure is not null)
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(levelsFailure).Throw();

    }

    private void ExpandBinaryDictionary(
        ReadOnlySpan<int> indices,
        ReadOnlySpan<byte> validityBits,
        bool optional,
        int rowCount,
        ParquetReaderOptions options,
        ParquetScanMemoryBudget budget,
        ref PooledArrayOwner<int>? pageBinaryOffsets,
        ref PooledArrayOwner<byte>? pageBinaryPayload,
        ParquetErrorLocation location,
        CancellationToken cancellationToken)
    {
        if (_binaryOffsets is null || _binaryPayload is null)
            throw new InvalidOperationException("The dictionary representation does not match its physical type.");

        // An empty bitmap means every row is valid: indices align one to one
        // with rows. Otherwise set bits select the consumed indices.
        var tight = !optional || validityBits.IsEmpty;
        var outputCount = optional ? rowCount : indices.Length;
        var dictionaryOffsets = _binaryOffsets.Memory.Span;
        var dictionaryCount = _count;
        long aggregateLength = 0;
        cancellationToken.ThrowIfCancellationRequested();
        if (tight)
        {
            if (optional && indices.Length != rowCount)
                throw new ParquetFormatException("Dictionary indices do not match the definition levels.", location);
            for (var position = 0; position < indices.Length; position++)
            {
                if ((position & 1023) == 0)
                    cancellationToken.ThrowIfCancellationRequested();
                var dictionaryIndex = indices[position];
                if ((uint)dictionaryIndex >= (uint)dictionaryCount)
                    throw new ParquetFormatException("A dictionary index is outside the dictionary.", location);
                aggregateLength += dictionaryOffsets[dictionaryIndex + 1] - dictionaryOffsets[dictionaryIndex];
            }
        }
        else
        {
            var physicalIndex = 0;
            for (var row = 0; row < rowCount; row++)
            {
                if ((row & 1023) == 0)
                    cancellationToken.ThrowIfCancellationRequested();
                if ((validityBits[row >> 3] & (1 << (row & 7))) == 0)
                    continue;
                var dictionaryIndex = indices[physicalIndex++];
                if ((uint)dictionaryIndex >= (uint)dictionaryCount)
                    throw new ParquetFormatException("A dictionary index is outside the dictionary.", location);
                aggregateLength += dictionaryOffsets[dictionaryIndex + 1] - dictionaryOffsets[dictionaryIndex];
            }
            if (physicalIndex != indices.Length)
                throw new ParquetFormatException("Dictionary indices do not match the definition levels.", location);
        }
        var retainedBytes = aggregateLength + checked((outputCount + 1L) * sizeof(int));
        if (aggregateLength > int.MaxValue || retainedBytes > options.MaximumScanPooledBytes)
            throw new ParquetLimitExceededException("An expanded BYTE_ARRAY dictionary page exceeds the configured scan-memory limit.", location);


        PooledArrayOwner<int>? offsets = null;
        PooledArrayOwner<byte>? payload = null;
        try
        {
            offsets = PooledArrayOwner<int>.Rent(checked(outputCount + 1), budget);
            payload = PooledArrayOwner<byte>.Rent((int)aggregateLength, budget);
            var outputOffsets = offsets.Memory.Span;
            var outputPayload = payload.Memory.Span;
            var dictionaryPayload = _binaryPayload.Memory.Span;
            outputOffsets[0] = 0;
            // The prescan above already range-checked every consumed index in
            // the same order, so the copies below reuse them directly. Null
            // slots repeat the running offset, keeping zero-length entries.
            var outputOffset = 0;
            if (tight)
            {
                for (var position = 0; position < indices.Length; position++)
                {
                    if ((position & 1023) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    var dictionaryIndex = indices[position];
                    var start = dictionaryOffsets[dictionaryIndex];
                    var length = dictionaryOffsets[dictionaryIndex + 1] - start;
                    dictionaryPayload.Slice(start, length)
                        .CopyTo(outputPayload[outputOffset..]);
                    outputOffset += length;
                    outputOffsets[position + 1] = outputOffset;
                }
            }
            else
            {
                var physicalIndex = 0;
                for (var row = 0; row < rowCount; row++)
                {
                    if ((row & 1023) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    if ((validityBits[row >> 3] & (1 << (row & 7))) == 0)
                    {
                        outputOffsets[row + 1] = outputOffset;
                        continue;
                    }
                    var dictionaryIndex = indices[physicalIndex++];
                    var start = dictionaryOffsets[dictionaryIndex];
                    var length = dictionaryOffsets[dictionaryIndex + 1] - start;
                    dictionaryPayload.Slice(start, length)
                        .CopyTo(outputPayload[outputOffset..]);
                    outputOffset += length;
                    outputOffsets[row + 1] = outputOffset;
                }
            }
            pageBinaryOffsets = offsets;
            pageBinaryPayload = payload;
            offsets = null;
            payload = null;
        }
        catch
        {
            // Best-effort rollback preserves the primary page error.
            try { offsets?.Dispose(); } catch (Exception) { }
            try { payload?.Dispose(); } catch (Exception) { }
            throw;
        }
    }

    private void ExpandFixedDictionary(
        ReadOnlySpan<int> indices,
        ReadOnlySpan<byte> validityBits,
        bool optional,
        int rowCount,
        ParquetReaderOptions options,
        ParquetScanMemoryBudget budget,
        ref PooledArrayOwner<byte>? pageFixedPayload,
        ref int pageFixedWidth,
        ParquetErrorLocation location,
        CancellationToken cancellationToken)
    {
        if (_fixedPayload is null || _fixedWidth <= 0)
            throw new InvalidOperationException("The dictionary representation does not match its physical type.");

        var tight = !optional || validityBits.IsEmpty;
        var outputCount = optional ? rowCount : indices.Length;
        var byteCount = checked((long)outputCount * _fixedWidth);
        if (byteCount > int.MaxValue || byteCount > options.MaximumScanPooledBytes)
            throw new ParquetLimitExceededException("An expanded FIXED_LEN_BYTE_ARRAY dictionary page exceeds the configured scan-memory limit.", location);


        PooledArrayOwner<byte>? payload = PooledArrayOwner<byte>.Rent((int)byteCount, budget);
        try
        {
            var dictionaryPayload = _fixedPayload.Memory.Span;
            var outputPayload = payload.Memory.Span;
            var fixedWidth = _fixedWidth;
            var dictionaryCount = _count;
            // The tight loops below overwrite every slot; only mixed pages
            // need cleared null slots.
            if (!tight)
                outputPayload.Clear();
            cancellationToken.ThrowIfCancellationRequested();
            if (tight)
            {
                if (optional && indices.Length != rowCount)
                    throw new ParquetFormatException("Dictionary indices do not match the definition levels.", location);
                for (var position = 0; position < indices.Length; position++)
                {
                    if ((position & 1023) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    var dictionaryIndex = indices[position];
                    if ((uint)dictionaryIndex >= (uint)dictionaryCount)
                        throw new ParquetFormatException("A dictionary index is outside the dictionary.", location);
                    dictionaryPayload
                        .Slice(dictionaryIndex * fixedWidth, fixedWidth)
                        .CopyTo(outputPayload.Slice(position * fixedWidth, fixedWidth));
                }
            }
            else
            {
                var physicalIndex = 0;
                for (var row = 0; row < rowCount; row++)
                {
                    if ((row & 1023) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    if ((validityBits[row >> 3] & (1 << (row & 7))) == 0)
                        continue;
                    var dictionaryIndex = indices[physicalIndex++];
                    if ((uint)dictionaryIndex >= (uint)dictionaryCount)
                        throw new ParquetFormatException("A dictionary index is outside the dictionary.", location);
                    dictionaryPayload
                        .Slice(dictionaryIndex * fixedWidth, fixedWidth)
                        .CopyTo(outputPayload.Slice(row * fixedWidth, fixedWidth));
                }
                if (physicalIndex != indices.Length)
                    throw new ParquetFormatException("Dictionary indices do not match the definition levels.", location);
            }
            pageFixedPayload = payload;
            pageFixedWidth = _fixedWidth;
            payload = null;
        }
        catch
        {
            // Best-effort rollback preserves the primary page error.
            try { payload?.Dispose(); } catch (Exception) { }
            throw;
        }
    }

    private void ExpandValues<T>(
        ReadOnlySpan<int> indices,
        ReadOnlySpan<byte> validityBits,
        bool optional,
        int rowCount,
        ref PooledValueLease pageValues,
        ParquetErrorLocation location,
        CancellationToken cancellationToken)
        where T : unmanaged
    {
        if (!_values.TryGetValues<T>(out var dictionary) || dictionary is null)
            throw new InvalidOperationException("The dictionary representation does not match its physical type.");

        var tight = !optional || validityBits.IsEmpty;
        var outputCount = optional ? rowCount : indices.Length;
        PooledArrayOwner<T>? rowValues = null;
        try
        {
            rowValues = _cacheProvider.RentColumnValues<T>(_column, outputCount);
            var output = rowValues.Memory.Span;
            // The tight loops below overwrite every slot; only mixed pages
            // need cleared null slots.
            if (!tight)
                output.Clear();
            cancellationToken.ThrowIfCancellationRequested();
            if (tight)
            {
                if (optional && indices.Length != rowCount)
                    throw new ParquetFormatException("Dictionary indices do not match the definition levels.", location);
                for (var position = 0; position < indices.Length; position++)
                {
                    if ((position & 1023) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    var dictionaryIndex = indices[position];
                    if ((uint)dictionaryIndex >= (uint)_count)
                        throw new ParquetFormatException("A dictionary index is outside the dictionary.", location);
                    output[position] = dictionary[dictionaryIndex];
                }
            }
            else
            {
                var physicalIndex = 0;
                for (var row = 0; row < rowCount; row++)
                {
                    if ((row & 1023) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    if ((validityBits[row >> 3] & (1 << (row & 7))) == 0)
                        continue;
                    var dictionaryIndex = indices[physicalIndex++];
                    if ((uint)dictionaryIndex >= (uint)_count)
                        throw new ParquetFormatException("A dictionary index is outside the dictionary.", location);
                    output[row] = dictionary[dictionaryIndex];
                }
                if (physicalIndex != indices.Length)
                    throw new ParquetFormatException("Dictionary indices do not match the definition levels.", location);
            }
            pageValues.Set(rowValues);
            rowValues = null;
        }
        catch
        {
            // Best-effort rollback preserves the primary page error.
            try { rowValues?.Dispose(); } catch (Exception) { }
            throw;
        }
    }

    // Row-group boundary: releases the dictionary store with the same
    // exhaustive first-failure policy as the cursor teardown.
    public void Reset() => Release();

    public void Dispose() => Release();

    private void Release()
    {
        Exception? failure = null;
        try { _values.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
        try { _binaryOffsets?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
        _binaryOffsets = null;
        try { _binaryPayload?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
        _binaryPayload = null;
        try { _fixedPayload?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
        _fixedPayload = null;
        _fixedWidth = 0;
        _count = 0;
        if (failure is not null)
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
    }
}


