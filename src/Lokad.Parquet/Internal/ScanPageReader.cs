namespace Lokad.Parquet.Internal;

// Cohesive page I/O component with narrow inputs and explicit ownership.
// It reads page headers with retry, performs exact source reads with
// truncation translation, lends borrowed slices of retained source memory for
// payloads when available, and computes page CRCs. Page payloads may be
// borrowed, but page headers are always rented into pooled buffers and filled
// by the read, never borrowed. The reader owns only its rented header buffers,
// which are returned on both parse success and parse failure; decoded payload
// ownership stays with the scan cursor. Page I/O stays in this dedicated scope
// with narrow inputs and explicit ownership, without changing the public API
// or the sequential single-scan contract.
internal static class ScanPageReader
{
    // Bounded 8 KiB slicing-by-8 IEEE tables shared by every page check,
    // initialized once by the static constructor on first use. SSE4.2 CRC32
    // is deliberately not used: it computes a different polynomial.
    private static readonly uint[][] Crc32Tables;

    static ScanPageReader()
    {
        var tables = new uint[8][];
        for (var table = 0; table < 8; table++)
            tables[table] = new uint[256];
        for (var value = 0; value < 256; value++)
        {
            var entry = (uint)value;
            for (var bit = 0; bit < 8; bit++)
                entry = (entry >> 1) ^ (0xEDB88320u & (uint)-(int)(entry & 1));
            tables[0][value] = entry;
        }
        for (var value = 0; value < 256; value++)
        {
            var entry = tables[0][value];
            for (var table = 1; table < 8; table++)
            {
                entry = tables[0][entry & 0xFF] ^ (entry >> 8);
                tables[table][value] = entry;
            }
        }
        Crc32Tables = tables;
    }

    internal static uint ComputeCrc32(ReadOnlySpan<byte> input, CancellationToken cancellationToken)
    {
        var tables = Crc32Tables;
        var crc = uint.MaxValue;
        var position = 0;
        var blockLimit = input.Length & ~7;
        while (position < blockLimit)
        {
            if ((position & 4095) == 0)
                cancellationToken.ThrowIfCancellationRequested();
            crc = tables[7][input[position] ^ (byte)crc]
                ^ tables[6][input[position + 1] ^ (byte)(crc >> 8)]
                ^ tables[5][input[position + 2] ^ (byte)(crc >> 16)]
                ^ tables[4][input[position + 3] ^ (byte)(crc >> 24)]
                ^ tables[3][input[position + 4]]
                ^ tables[2][input[position + 5]]
                ^ tables[1][input[position + 6]]
                ^ tables[0][input[position + 7]];
            position += 8;
        }
        while (position < input.Length)
        {
            if ((position & 4095) == 0)
                cancellationToken.ThrowIfCancellationRequested();
            crc = tables[0][(int)((crc ^ input[position]) & 0xFF)] ^ (crc >> 8);
            position++;
        }
        return ~crc;
    }

    internal static bool TryGetSourceMemory(
        IParquetRandomAccessSource source,
        long sourceLength,
        long offset,
        int count,
        out ReadOnlyMemory<byte> memory)
    {
        if (source is MemoryRandomAccessSource memorySource)
        {
            SourceRange.Validate(sourceLength, offset, count);
            memory = memorySource.Content.Slice(checked((int)offset), count);
            return true;
        }
        if (source is StreamRandomAccessSource { MemoryBuffer: { } memoryBuffer })
        {
            SourceRange.Validate(sourceLength, offset, count);
            memory = memoryBuffer.AsMemory(checked((int)offset), count);
            return true;
        }
        memory = default;
        return false;
    }

    internal static ValueTask<ParsedPageHeader> ReadPageHeaderAsync(
        IParquetRandomAccessSource source,
        ParquetReaderOptions options,
        PooledArrayOwnerCache<byte> headerCache,
        CancellationToken cancellationToken,
        long pageOffset,
        long chunkEnd,
        int rowGroupOrdinal,
        int columnOrdinal,
        int pageOrdinal)
    {
        var remaining = chunkEnd - pageOffset;
        var maximum = checked((int)Math.Min(remaining, options.MaximumPageHeaderBytes));
        if (maximum <= 0)
            throw new ParquetFormatException("A page header has no bytes available.", ParquetErrorLocation.AtPage(pageOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal));
        return ReadAtLength(
            source,
            options,
            headerCache,
            cancellationToken,
            pageOffset,
            rowGroupOrdinal,
            columnOrdinal,
            pageOrdinal,
            remaining,
            maximum,
            Math.Min(256, maximum));

        static ValueTask<ParsedPageHeader> ReadAtLength(
            IParquetRandomAccessSource source,
            ParquetReaderOptions options,
            PooledArrayOwnerCache<byte> headerCache,
            CancellationToken cancellationToken,
            long pageOffset,
            int rowGroupOrdinal,
            int columnOrdinal,
            int pageOrdinal,
            long remaining,
            int maximum,
            int length)
        {
            var owner = headerCache.Rent(length);
            ValueTask reading;
            try
            {
                reading = ReadExactlyAsync(
                    source,
                    cancellationToken,
                    pageOffset,
                    new ArraySegment<byte>(owner.Array, 0, owner.Memory.Length),
                    "The immutable input ended during a page read.",
                    ParquetErrorLocation.AtPage(pageOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal));
            }
            catch
            {
                owner.Dispose();
                throw;
            }
            if (!reading.IsCompletedSuccessfully)
                return AwaitReadAsync(
                    reading,
                    owner,
                    source,
                    options,
                    headerCache,
                    cancellationToken,
                    pageOffset,
                    rowGroupOrdinal,
                    columnOrdinal,
                    pageOrdinal,
                    remaining,
                    maximum,
                    length);

            // Header reads retry with doubled length up to the chunk/limit maximum;
            // the rented header buffer is returned on both parse success and parse failure.
            bool parsedSuccessfully;
            ParsedPageHeader parsed;
            try
            {
                parsedSuccessfully = PageHeaderParser.TryParse(
                    owner.Memory.Span,
                    pageOffset,
                    options,
                    cancellationToken,
                    out parsed);
            }
            catch (ParquetFormatException exception) when (exception.RowGroupOrdinal is null)
            {
                throw new ParquetFormatException(
                    exception.Message,
                    exception,
                    ParquetErrorLocation.AtPage(exception.ByteOffset ?? pageOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal));
            }
            catch (ParquetLimitExceededException exception) when (exception.RowGroupOrdinal is null)
            {
                throw new ParquetLimitExceededException(
                    exception.Message,
                    ParquetErrorLocation.AtPage(exception.ByteOffset ?? pageOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal));
            }
            finally
            {
                owner.Dispose();
            }
            if (parsedSuccessfully)
                return ValueTask.FromResult(parsed);
            if (length == maximum)
                throw HeaderExhausted(
                    options,
                    remaining,
                    pageOffset,
                    rowGroupOrdinal,
                    columnOrdinal,
                    pageOrdinal);
            return ReadAtLength(
                source,
                options,
                headerCache,
                cancellationToken,
                pageOffset,
                rowGroupOrdinal,
                columnOrdinal,
                pageOrdinal,
                remaining,
                maximum,
                Math.Min(maximum, checked(length * 2)));
        }

        static async ValueTask<ParsedPageHeader> AwaitReadAsync(
            ValueTask reading,
            PooledArrayOwner<byte> owner,
            IParquetRandomAccessSource source,
            ParquetReaderOptions options,
            PooledArrayOwnerCache<byte> headerCache,
            CancellationToken cancellationToken,
            long pageOffset,
            int rowGroupOrdinal,
            int columnOrdinal,
            int pageOrdinal,
            long remaining,
            int maximum,
            int length)
        {
            try
            {
                await reading.ConfigureAwait(false);
                if (PageHeaderParser.TryParse(
                    owner.Memory.Span,
                    pageOffset,
                    options,
                    cancellationToken,
                    out var parsed))
                    return parsed;
            }
            catch (ParquetFormatException exception) when (exception.RowGroupOrdinal is null)
            {
                throw new ParquetFormatException(
                    exception.Message,
                    exception,
                    ParquetErrorLocation.AtPage(exception.ByteOffset ?? pageOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal));
            }
            catch (ParquetLimitExceededException exception) when (exception.RowGroupOrdinal is null)
            {
                throw new ParquetLimitExceededException(
                    exception.Message,
                    ParquetErrorLocation.AtPage(exception.ByteOffset ?? pageOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal));
            }
            finally
            {
                owner.Dispose();
            }
            if (length == maximum)
                throw HeaderExhausted(
                    options,
                    remaining,
                    pageOffset,
                    rowGroupOrdinal,
                    columnOrdinal,
                    pageOrdinal);
            return await ReadAtLength(
                source,
                options,
                headerCache,
                cancellationToken,
                pageOffset,
                rowGroupOrdinal,
                columnOrdinal,
                pageOrdinal,
                remaining,
                maximum,
                Math.Min(maximum, checked(length * 2))).ConfigureAwait(false);
        }

        static Exception HeaderExhausted(
            ParquetReaderOptions options,
            long remaining,
            long pageOffset,
            int rowGroupOrdinal,
            int columnOrdinal,
            int pageOrdinal)
        {
            if (remaining <= options.MaximumPageHeaderBytes)
                return new ParquetFormatException(
                    "A page header exceeds its enclosing column chunk.",
                    ParquetErrorLocation.AtPage(pageOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal));

            return new ParquetLimitExceededException(
                "A page header exceeds the configured byte limit.",
                ParquetErrorLocation.AtPage(pageOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal));
        }
    }

    internal static ValueTask ReadExactlyAsync(
        IParquetRandomAccessSource source,
        CancellationToken cancellationToken,
        long offset,
        ArraySegment<byte> destination,
        string truncationMessage,
        ParquetErrorLocation location)
    {
        ValueTask read;
        try
        {
            read = source.ReadExactlyAsync(offset, destination, cancellationToken);
        }
        catch (EndOfStreamException exception)
        {
            throw new ParquetFormatException(
                truncationMessage, exception, location);
        }
        if (read.IsCompletedSuccessfully)
        {
            try
            {
                read.GetAwaiter().GetResult();
            }
            catch (EndOfStreamException exception)
            {
                throw new ParquetFormatException(
                    truncationMessage, exception, location);
            }

            return ValueTask.CompletedTask;
        }

        return AwaitReadAsync(read, truncationMessage, location);

        static async ValueTask AwaitReadAsync(ValueTask pendingRead, string truncationMessage, ParquetErrorLocation location)
        {
            try
            {
                await pendingRead.ConfigureAwait(false);
            }
            catch (EndOfStreamException exception)
            {
                throw new ParquetFormatException(
                    truncationMessage, exception, location);
            }
        }
    }
}



