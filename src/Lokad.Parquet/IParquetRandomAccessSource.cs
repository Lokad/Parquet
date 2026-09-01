namespace Lokad.Parquet;

/// <summary>Provides exact asynchronous reads over immutable random-access content.</summary>
public interface IParquetRandomAccessSource : IAsyncDisposable
{
    /// <summary>Gets the immutable source length in bytes.</summary>
    long Length { get; }

    /// <summary>Reads exactly <paramref name="destination"/> from <paramref name="offset"/>.</summary>
    /// <param name="offset">The non-negative source offset.</param>
    /// <param name="destination">The destination buffer.</param>
    /// <param name="cancellationToken">Cancellation for the read.</param>
    /// <returns>A task that completes only after the destination is full.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The requested range is outside the source.</exception>
    /// <exception cref="EndOfStreamException">The source ended before the destination was full.</exception>
    ValueTask ReadExactlyAsync(
        long offset,
        ArraySegment<byte> destination,
        CancellationToken cancellationToken);
}
