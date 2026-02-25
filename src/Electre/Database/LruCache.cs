namespace Electre.Database;

/// <summary>
///     Generic least-recently-used (LRU) cache with thread-safe operations.
/// </summary>
/// <typeparam name="TKey">Type of the cache key.</typeparam>
/// <typeparam name="TValue">Type of the cached value.</typeparam>
public sealed class LruCache<TKey, TValue> where TKey : notnull
{
    /// <summary>
    ///     Dictionary mapping keys to linked list nodes for O(1) access.
    /// </summary>
    private readonly Dictionary<TKey, LinkedListNode<CacheEntry>> _cache;

    /// <summary>
    ///     Lock for thread-safe operations.
    /// </summary>
    private readonly object _lock = new();

    /// <summary>
    ///     Linked list maintaining LRU order (most recent at head, least recent at tail).
    /// </summary>
    private readonly LinkedList<CacheEntry> _lruList;

    /// <summary>
    ///     Maximum number of entries allowed in the cache.
    /// </summary>
    private readonly int _maxSize;

    /// <summary>
    ///     Initializes a new LruCache instance.
    /// </summary>
    /// <param name="maxSize">Maximum number of entries allowed in the cache.</param>
    public LruCache(int maxSize)
    {
        _maxSize = maxSize;
        _cache = new Dictionary<TKey, LinkedListNode<CacheEntry>>(maxSize);
        _lruList = new LinkedList<CacheEntry>();
    }

    /// <summary>
    ///     Gets the current number of entries in the cache.
    /// </summary>
    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _cache.Count;
            }
        }
    }

    /// <summary>
    ///     Tries to get a value from the cache.
    /// </summary>
    /// <param name="key">Cache key.</param>
    /// <param name="value">Output value if found.</param>
    /// <returns>True if the key exists in the cache, false otherwise.</returns>
    public bool TryGet(TKey key, out TValue? value)
    {
        lock (_lock)
        {
            if (_cache.TryGetValue(key, out var node))
            {
                _lruList.Remove(node);
                _lruList.AddFirst(node);
                value = node.Value.Value;
                return true;
            }

            value = default;
            return false;
        }
    }

    /// <summary>
    ///     Sets a value in the cache, evicting the least recently used entry if necessary.
    /// </summary>
    /// <param name="key">Cache key.</param>
    /// <param name="value">Value to cache.</param>
    public void Set(TKey key, TValue value)
    {
        lock (_lock)
        {
            if (_cache.TryGetValue(key, out var existingNode))
            {
                existingNode.Value = new CacheEntry(key, value);
                _lruList.Remove(existingNode);
                _lruList.AddFirst(existingNode);
                return;
            }

            if (_cache.Count >= _maxSize)
                EvictOldest();

            var entry = new CacheEntry(key, value);
            var node = _lruList.AddFirst(entry);
            _cache[key] = node;
        }
    }

    /// <summary>
    ///     Removes a key from the cache.
    /// </summary>
    /// <param name="key">Cache key to remove.</param>
    public void Remove(TKey key)
    {
        lock (_lock)
        {
            if (_cache.TryGetValue(key, out var node))
            {
                _lruList.Remove(node);
                _cache.Remove(key);
            }
        }
    }

    /// <summary>
    ///     Clears all entries from the cache.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            _cache.Clear();
            _lruList.Clear();
        }
    }

    /// <summary>
    ///     Evicts the least recently used entry from the cache.
    /// </summary>
    private void EvictOldest()
    {
        var oldest = _lruList.Last;
        if (oldest is not null)
        {
            _lruList.RemoveLast();
            _cache.Remove(oldest.Value.Key);
        }
    }

    /// <summary>
    ///     Cache entry containing a key-value pair.
    /// </summary>
    private readonly record struct CacheEntry(TKey Key, TValue Value);
}