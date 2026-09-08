namespace Lokad.Parquet.Internal;

// Owned-versus-borrowed payload lease. Transitions are None -> Owned/Borrowed
// -> None (transferred or disposed). Kind distinguishes an empty borrowed
// payload from no payload, replacing the previous empty-memory sentinel.
// This struct holds no pooled ownership itself beyond the single owner it
// leases; it never appears in public batch owner arrays.
internal enum PagePayloadKind
{
    None,
    Owned,
    Borrowed,
}

internal struct PagePayloadLease
{
    private PooledArrayOwner<byte>? _owner;
    private ReadOnlyMemory<byte> _borrowed;
    private PagePayloadKind _kind;

    public PagePayloadKind Kind => _kind;

    public bool HasPayload => _kind != PagePayloadKind.None;

    public ReadOnlyMemory<byte> Memory
    {
        get
        {
            if (_kind == PagePayloadKind.Owned)
            {
                if (_owner is PooledArrayOwner<byte> owned)
                    return owned.Memory;
                throw new InvalidOperationException("An owned page payload has no owner.");
            }
            if (_kind == PagePayloadKind.Borrowed)
                return _borrowed;
            return ReadOnlyMemory<byte>.Empty;
        }
    }

    public ReadOnlySpan<byte> Span
    {
        get
        {
            if (_kind == PagePayloadKind.Owned)
            {
                if (_owner is PooledArrayOwner<byte> owned)
                    return owned.Memory.Span;
                throw new InvalidOperationException("An owned page payload has no owner.");
            }
            if (_kind == PagePayloadKind.Borrowed)
                return _borrowed.Span;
            return ReadOnlySpan<byte>.Empty;
        }
    }

    public void SetOwned(PooledArrayOwner<byte> owner)
    {
        ArgumentNullException.ThrowIfNull(owner);
        if (_kind != PagePayloadKind.None)
            throw new InvalidOperationException("A page payload is already loaded.");
        _owner = owner;
        _kind = PagePayloadKind.Owned;
    }

    public void SetBorrowed(ReadOnlyMemory<byte> memory)
    {
        if (_kind != PagePayloadKind.None)
            throw new InvalidOperationException("A page payload is already loaded.");
        _borrowed = memory;
        _kind = PagePayloadKind.Borrowed;
    }

    public void Dispose()
    {
        try
        {
            _owner?.Dispose();
        }
        finally
        {
            _owner = null;
            _borrowed = default;
            _kind = PagePayloadKind.None;
        }
    }
}

// Explicit page-validity state. Transitions are None (no page loaded) ->
// AllValid/Explicit (page loaded) -> None (transferred or disposed).
// AllValid means implicitly all-valid with no bitmap; Explicit means the owner
// holds the bitmap. This replaces the previous convention where a null bitmap
// field meant required, optional all-valid with a dropped bitmap, and no page.
internal enum PageValidityKind
{
    None,
    AllValid,
    Explicit,
}

internal struct PageValidityState
{
    private PooledArrayOwner<byte>? _owner;
    private PageValidityKind _kind;

    public PageValidityKind Kind => _kind;

    public bool IsLoaded => _kind != PageValidityKind.None;

    public bool IsAllValid => _kind == PageValidityKind.AllValid;

    public ReadOnlyMemory<byte> Bits
    {
        get
        {
            if (_kind == PageValidityKind.Explicit)
            {
                if (_owner is PooledArrayOwner<byte> owned)
                    return owned.Memory;
                throw new InvalidOperationException("An explicit page validity has no owner.");
            }
            if (_kind == PageValidityKind.AllValid)
                return ReadOnlyMemory<byte>.Empty;
            throw new InvalidOperationException("No page validity is loaded.");
        }
    }

    public void SetAllValid()
    {
        if (_kind != PageValidityKind.None)
            throw new InvalidOperationException("A page validity is already loaded.");
        _kind = PageValidityKind.AllValid;
    }

    public void SetExplicit(PooledArrayOwner<byte> owner)
    {
        ArgumentNullException.ThrowIfNull(owner);
        if (_kind != PageValidityKind.None)
            throw new InvalidOperationException("A page validity is already loaded.");
        _owner = owner;
        _kind = PageValidityKind.Explicit;
    }

    public void SetFromNullable(PooledArrayOwner<byte>? owner)
    {
        if (_kind != PageValidityKind.None)
            throw new InvalidOperationException("A page validity is already loaded.");
        if (owner is null)
        {
            _kind = PageValidityKind.AllValid;
            return;
        }
        _owner = owner;
        _kind = PageValidityKind.Explicit;
    }

    public PooledArrayOwner<byte>? Take()
    {
        if (_kind == PageValidityKind.None)
            throw new InvalidOperationException("No page validity is loaded.");
        var owner = _owner;
        _owner = null;
        _kind = PageValidityKind.None;
        return owner;
    }

    public void Dispose()
    {
        try
        {
            _owner?.Dispose();
        }
        finally
        {
            _owner = null;
            _kind = PageValidityKind.None;
        }
    }
}
