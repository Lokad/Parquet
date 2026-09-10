namespace Lokad.Parquet.Internal;

// Payload buffer states. Only None is empty: Borrowed covers an empty borrowed
// slice too, so Kind alone tells whether a page buffer is loaded.
internal enum PagePayloadKind
{
    None,
    Owned,
    Borrowed,
}

// Mutable lease over one decoded page buffer. Owned holds a pooled array owner
// released by Dispose; Borrowed holds a slice of retained source memory and
// releases nothing. SetOwned and SetBorrowed each require None; Dispose returns
// a rented owner, if any, and resets to None for the next page. The cursor keeps
// the single instance in a private field, mutates it in place, and never copies
// it or passes it by value, so the rented buffer cannot escape through a struct
// copy; decoders read through Span while batch storage stays separate.
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

// Rented primitive value buffer for one page or dictionary. The owner and its
// array are set and cleared together, so a value can never outlive or mismatch
// its owner. TryTake hands the owner to a full-page batch and resets, while
// TryGetValues exposes the array for partial copies without releasing it. Set
// requires an empty lease. The cursor keeps the single instance in a private
// field and never copies it.
internal struct PooledValueLease
{
    private object? _owner;
    private Array? _values;

    public bool HasValues => _owner is not null;

    public void Set<T>(PooledArrayOwner<T> owner)
        where T : unmanaged
    {
        ArgumentNullException.ThrowIfNull(owner);
        if (_owner is not null)
            throw new InvalidOperationException("A page value buffer is already loaded.");
        _owner = owner;
        _values = owner.Array;
    }

    public bool TryTake<T>(out PooledArrayOwner<T>? owner)
        where T : unmanaged
    {
        if (_owner is PooledArrayOwner<T> typed)
        {
            owner = typed;
            _owner = null;
            _values = null;
            return true;
        }
        owner = null;
        return false;
    }

    public bool TryGetValues<T>(out T[]? values)
        where T : unmanaged
    {
        values = _values as T[];
        return values is not null;
    }

    public void Dispose()
    {
        try
        {
            (_owner as IDisposable)?.Dispose();
        }
        finally
        {
            _owner = null;
            _values = null;
        }
    }
}

// Page validity states. Only None means no page is loaded; AllValid carries
// no bitmap at all.
internal enum PageValidityKind
{
    None,
    AllValid,
    Explicit,
}

// Mutable validity holder for one page. AllValid records implicit all-validity
// with no bitmap; Explicit owns the bitmap released by Dispose. The Set methods
// each require None; Take hands the bitmap owner to the batch, if any, and resets
// to None, while Dispose releases it in place. Like the payload lease, the cursor
// keeps the single instance in a private field and never copies it.
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

