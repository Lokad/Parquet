using Microsoft.Win32.SafeHandles;

namespace Lokad.Parquet.Internal;

internal static class SourceRange
{
    public static void Validate(long length, long offset, int count)
    {
        if (offset < 0)
            throw new ArgumentOutOfRangeException(nameof(offset));
        if (count < 0 || offset > length || count > length - offset)
            throw new ArgumentOutOfRangeException(nameof(count), "The requested range is outside the source.");
    }
}

internal sealed class FileRandomAccessSource : IParquetRandomAccessSource
{
    private readonly SafeFileHandle _handle;
    private bool _disposed;

    public FileRandomAccessSource(string path)
    {
        _handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        Length = RandomAccess.GetLength(_handle);
    }

    public long Length { get; }

    public async ValueTask ReadExactlyAsync(
        long offset,
        ArraySegment<byte> destination,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        SourceRange.Validate(Length, offset, destination.Count);

        var read = 0;
        while (read < destination.Count)
        {
            var count = await RandomAccess.ReadAsync(
                _handle,
                destination.AsMemory(read),
                checked(offset + read),
                cancellationToken).ConfigureAwait(false);
            if (count == 0)
                throw new EndOfStreamException();
            read += count;
        }
    }

    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _handle.Dispose();
        }
        return ValueTask.CompletedTask;
    }
}

internal sealed class MemoryRandomAccessSource : IParquetRandomAccessSource
{
    private readonly ReadOnlyMemory<byte> _content;
    private bool _disposed;

    public MemoryRandomAccessSource(ReadOnlyMemory<byte> content) => _content = content;

    public long Length => _content.Length;

    internal ReadOnlyMemory<byte> Content
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _content;
        }
    }

    public ValueTask ReadExactlyAsync(
        long offset,
        ArraySegment<byte> destination,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        SourceRange.Validate(Length, offset, destination.Count);
        cancellationToken.ThrowIfCancellationRequested();
        _content.Slice(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }
}

internal sealed class StreamRandomAccessSource : IParquetRandomAccessSource
{
    private readonly Stream _stream;
    private readonly ParquetSourceOwnership _ownership;
    private readonly ArraySegment<byte>? _memoryBuffer;
    private bool _disposed;

    public StreamRandomAccessSource(Stream stream, ParquetSourceOwnership ownership)
    {
        if (!stream.CanRead)
            throw new ArgumentException("The stream must be readable.", nameof(stream));
        if (!stream.CanSeek)
            throw new ArgumentException("The stream must be seekable.", nameof(stream));

        _stream = stream;
        _ownership = ownership;
        // Only the exact MemoryStream implementation has safe buffer/disposal shortcuts; subclasses may override behavior.
        if (stream.GetType() == typeof(MemoryStream) && stream is MemoryStream memoryStream && memoryStream.TryGetBuffer(out var memoryBuffer))
            _memoryBuffer = memoryBuffer;
        Length = stream.Length;
        if (Length < 0)
            throw new ArgumentException("The stream length must be non-negative.", nameof(stream));
    }

    public long Length { get; }

    internal bool CanDisposeSynchronously =>
        (_ownership == ParquetSourceOwnership.Caller || _stream.GetType() == typeof(MemoryStream)) && !_disposed;

    internal ArraySegment<byte>? MemoryBuffer
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _memoryBuffer;
        }
    }

    public ValueTask ReadExactlyAsync(
        long offset,
        ArraySegment<byte> destination,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        SourceRange.Validate(Length, offset, destination.Count);
        cancellationToken.ThrowIfCancellationRequested();

        if (_memoryBuffer is { } memoryBuffer)
        {
            memoryBuffer.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
            return ValueTask.CompletedTask;
        }

        if (_stream.GetType() == typeof(MemoryStream))
        {
            ReadMemoryStream();
            return ValueTask.CompletedTask;
        }

        return ReadSlowAsync();

        void ReadMemoryStream()
        {
            _stream.Position = offset;
            var read = 0;
            while (read < destination.Count)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var count = _stream.Read(destination.AsSpan(read));
                if (count == 0)
                    throw new EndOfStreamException();
                read += count;
            }
        }

        async ValueTask ReadSlowAsync()
        {
            _stream.Position = offset;
            var read = 0;
            while (read < destination.Count)
            {
                var count = await _stream.ReadAsync(
                    destination.AsMemory(read),
                    cancellationToken).ConfigureAwait(false);
                if (count == 0)
                    throw new EndOfStreamException();
                read += count;
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        _disposed = true;
        if (_ownership == ParquetSourceOwnership.Caller)
            return ValueTask.CompletedTask;
        return _stream.DisposeAsync();
    }
}
