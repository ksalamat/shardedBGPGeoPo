//
// Created by Kavé on 29/11/2020.
//

#ifndef BGPGEOPOLITICS_LRUCACHE_H
#define BGPGEOPOLITICS_LRUCACHE_H
#include <limits>
#include <memory>
#include <atomic>
#include <mutex>
#include <new>
#include <thread>
#include <vector>
#include <stdint.h>
#include "tbb/concurrent_hash_map.h"

namespace MurmurHash3 {

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

#define FORCE_INLINE inline __attribute__((always_inline))

    inline uint32_t rotl32 ( uint32_t x, int8_t r )
    {
        return (x << r) | (x >> (32 - r));
    }

    inline uint64_t rotl64 ( uint64_t x, int8_t r )
    {
        return (x << r) | (x >> (64 - r));
    }

#define ROTL32(x,y) rotl32(x,y)
#define ROTL64(x,y) rotl64(x,y)

//-----------------------------------------------------------------------------
// Block read - if your platform needs to do endian-swapping or can only
// handle aligned reads, do the conversion here

    FORCE_INLINE uint32_t getblock32 ( const uint32_t * p, int i )
    {
        return p[i];
    }

    FORCE_INLINE uint64_t getblock64 ( const uint64_t * p, int i )
    {
        return p[i];
    }

//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche

    FORCE_INLINE uint32_t fmix32 ( uint32_t h )
    {
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;

        return h;
    }

//----------

    FORCE_INLINE uint64_t fmix64 ( uint64_t k )
    {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdLLU;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53LLU;
        k ^= k >> 33;

        return k;
    }

//-----------------------------------------------------------------------------

    inline static void hash_x86_128 ( const void * key, const int len,
                                      uint32_t seed, void * out )
    {
        const uint8_t * data = (const uint8_t*)key;
        const int nblocks = len / 16;

        uint32_t h1 = seed;
        uint32_t h2 = seed;
        uint32_t h3 = seed;
        uint32_t h4 = seed;

        const uint32_t c1 = 0x239b961b;
        const uint32_t c2 = 0xab0e9789;
        const uint32_t c3 = 0x38b34ae5;
        const uint32_t c4 = 0xa1e38b93;

        //----------
        // body

        const uint32_t * blocks = (const uint32_t *)(data + nblocks*16);

        for(int i = -nblocks; i; i++)
        {
            uint32_t k1 = getblock32(blocks,i*4+0);
            uint32_t k2 = getblock32(blocks,i*4+1);
            uint32_t k3 = getblock32(blocks,i*4+2);
            uint32_t k4 = getblock32(blocks,i*4+3);

            k1 *= c1; k1  = ROTL32(k1,15); k1 *= c2; h1 ^= k1;

            h1 = ROTL32(h1,19); h1 += h2; h1 = h1*5+0x561ccd1b;

            k2 *= c2; k2  = ROTL32(k2,16); k2 *= c3; h2 ^= k2;

            h2 = ROTL32(h2,17); h2 += h3; h2 = h2*5+0x0bcaa747;

            k3 *= c3; k3  = ROTL32(k3,17); k3 *= c4; h3 ^= k3;

            h3 = ROTL32(h3,15); h3 += h4; h3 = h3*5+0x96cd1c35;

            k4 *= c4; k4  = ROTL32(k4,18); k4 *= c1; h4 ^= k4;

            h4 = ROTL32(h4,13); h4 += h1; h4 = h4*5+0x32ac3b17;
        }

        //----------
        // tail

        const uint8_t * tail = (const uint8_t*)(data + nblocks*16);

        uint32_t k1 = 0;
        uint32_t k2 = 0;
        uint32_t k3 = 0;
        uint32_t k4 = 0;

        switch(len & 15)
        {
            case 15: k4 ^= tail[14] << 16;
            case 14: k4 ^= tail[13] << 8;
            case 13: k4 ^= tail[12] << 0;
                k4 *= c4; k4  = ROTL32(k4,18); k4 *= c1; h4 ^= k4;

            case 12: k3 ^= tail[11] << 24;
            case 11: k3 ^= tail[10] << 16;
            case 10: k3 ^= tail[ 9] << 8;
            case  9: k3 ^= tail[ 8] << 0;
                k3 *= c3; k3  = ROTL32(k3,17); k3 *= c4; h3 ^= k3;

            case  8: k2 ^= tail[ 7] << 24;
            case  7: k2 ^= tail[ 6] << 16;
            case  6: k2 ^= tail[ 5] << 8;
            case  5: k2 ^= tail[ 4] << 0;
                k2 *= c2; k2  = ROTL32(k2,16); k2 *= c3; h2 ^= k2;

            case  4: k1 ^= tail[ 3] << 24;
            case  3: k1 ^= tail[ 2] << 16;
            case  2: k1 ^= tail[ 1] << 8;
            case  1: k1 ^= tail[ 0] << 0;
                k1 *= c1; k1  = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
        };

        //----------
        // finalization

        h1 ^= len; h2 ^= len; h3 ^= len; h4 ^= len;

        h1 += h2; h1 += h3; h1 += h4;
        h2 += h1; h3 += h1; h4 += h1;

        h1 = fmix32(h1);
        h2 = fmix32(h2);
        h3 = fmix32(h3);
        h4 = fmix32(h4);

        h1 += h2; h1 += h3; h1 += h4;
        h2 += h1; h3 += h1; h4 += h1;

        ((uint32_t*)out)[0] = h1;
        ((uint32_t*)out)[1] = h2;
        ((uint32_t*)out)[2] = h3;
        ((uint32_t*)out)[3] = h4;
    }

//-----------------------------------------------------------------------------

    inline static void hash_x64_128 ( const void * key, const int len,
                                      const uint32_t seed, void * out )
    {
        const uint8_t * data = (const uint8_t*)key;
        const int nblocks = len / 16;

        uint64_t h1 = seed;
        uint64_t h2 = seed;

        const uint64_t c1 = 0x87c37b91114253d5LLU;
        const uint64_t c2 = 0x4cf5ad432745937fLLU;

        //----------
        // body

        const uint64_t * blocks = (const uint64_t *)(data);

        for(int i = 0; i < nblocks; i++)
        {
            uint64_t k1 = getblock64(blocks,i*2+0);
            uint64_t k2 = getblock64(blocks,i*2+1);

            k1 *= c1; k1  = ROTL64(k1,31); k1 *= c2; h1 ^= k1;

            h1 = ROTL64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

            k2 *= c2; k2  = ROTL64(k2,33); k2 *= c1; h2 ^= k2;

            h2 = ROTL64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
        }

        //----------
        // tail

        const uint8_t * tail = (const uint8_t*)(data + nblocks*16);

        uint64_t k1 = 0;
        uint64_t k2 = 0;

        switch(len & 15)
        {
            case 15: k2 ^= ((uint64_t)tail[14]) << 48;
            case 14: k2 ^= ((uint64_t)tail[13]) << 40;
            case 13: k2 ^= ((uint64_t)tail[12]) << 32;
            case 12: k2 ^= ((uint64_t)tail[11]) << 24;
            case 11: k2 ^= ((uint64_t)tail[10]) << 16;
            case 10: k2 ^= ((uint64_t)tail[ 9]) << 8;
            case  9: k2 ^= ((uint64_t)tail[ 8]) << 0;
                k2 *= c2; k2  = ROTL64(k2,33); k2 *= c1; h2 ^= k2;

            case  8: k1 ^= ((uint64_t)tail[ 7]) << 56;
            case  7: k1 ^= ((uint64_t)tail[ 6]) << 48;
            case  6: k1 ^= ((uint64_t)tail[ 5]) << 40;
            case  5: k1 ^= ((uint64_t)tail[ 4]) << 32;
            case  4: k1 ^= ((uint64_t)tail[ 3]) << 24;
            case  3: k1 ^= ((uint64_t)tail[ 2]) << 16;
            case  2: k1 ^= ((uint64_t)tail[ 1]) << 8;
            case  1: k1 ^= ((uint64_t)tail[ 0]) << 0;
                k1 *= c1; k1  = ROTL64(k1,31); k1 *= c2; h1 ^= k1;
        };

        //----------
        // finalization

        h1 ^= len; h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        ((uint64_t*)out)[0] = h1;
        ((uint64_t*)out)[1] = h2;
    }

// HHVM-compatible interface
    template <bool unused>
    inline void hash128 ( const void * key, const int len,
                          const uint32_t seed, uint64_t out[2] )
    {
        if (std::numeric_limits<size_t>::digits == 32) {
            hash_x86_128(key, len, seed, (void*)out);
        } else {
            hash_x64_128(key, len, seed, (void*)out);
        }
    }


//-----------------------------------------------------------------------------

} // namespace MurmurHash3

struct ThreadSafeStringKey {
    ThreadSafeStringKey(const char* data, size_t size)
            : m_storage(new Storage(data, size))
    {}

    ThreadSafeStringKey() {}

    uint64_t hash() const {
        return m_storage->hash();
    }

    size_t size() const {
        return m_storage->m_size;
    }

    const char* data() const {
        return m_storage->m_data;
    }

    const char* c_str() const {
        return data();
    }

    bool operator==(const ThreadSafeStringKey& other) const {
        size_t s = size();
        return s == other.size() && 0 == std::memcmp(data(), other.data(), s);
    }

    struct HashCompare {
        bool equal(const ThreadSafeStringKey& j, const ThreadSafeStringKey& k) const {
            return j == k;
        }

        size_t hash(const ThreadSafeStringKey& k) const {
            return k.hash();
        }
    };

private:
    struct Storage {
        Storage(const char* data, size_t size)
                : m_size(size), m_hash(0)
        {
            m_data = new char[size + 1];
            memcpy(m_data, data, size);
            m_data[size] = '\0';
        }

        ~Storage() {
            delete[] m_data;
        }

        char* m_data;
        size_t m_size;
        mutable std::atomic<size_t> m_hash;

        size_t hash() const {
            size_t h = m_hash.load(std::memory_order_relaxed);
            if (h == 0) {
                uint64_t h128[2];
                MurmurHash3::hash128<false>(m_data, m_size, 0, h128);
                h = (size_t)h128[0];
                if (h == 0) {
                    h = 1;
                }
                m_hash.store(h, std::memory_order_relaxed);
            }
            return h;
        }
    };

    std::shared_ptr<Storage> m_storage;
};

/**
 * ThreadSafeLRUCache is a thread-safe hashtable with a limited size. When
 * it is full, insert() evicts the least recently used item from the cache.
 *
 * The find() operation fills a ConstAccessor object, which is a smart pointer
 * similar to TBB's const_accessor. After eviction, destruction of the value is
 * deferred until all ConstAccessor objects are destroyed.
 *
 * The implementation is generally conservative, relying on the documented
 * behaviour of tbb::concurrent_hash_map. LRU list transactions are protected
 * with a single mutex. Having our own doubly-linked list implementation helps
 * to ensure that list transactions are sufficiently brief, consisting of only
 * a few loads and stores. User code is not executed while the lock is held.
 *
 * The acquisition of the list mutex during find() is non-blocking (try_lock),
 * so under heavy lookup load, the container will not stall, instead some LRU
 * update operations will be omitted.
 *
 * Insert performance was observed to degrade rapidly when there is a heavy
 * concurrent insert/evict load, mostly due to locks in the underlying
 * TBB::CHM. So if that is a possibility for your workload,
 * ThreadSafeScalableCache is recommended instead.
 */
template <class TKey, class TValue, class THash = tbb::tbb_hash_compare<TKey>>
class ThreadSafeLRUCache {
    /**
     * The LRU list node.
     *
     * We make a copy of the key in the list node, allowing us to find the
     * TBB::CHM element from the list node. TBB::CHM invalidates iterators
     * on most operations, even find(), ruling out more efficient
     * implementations.
     */
    struct ListNode {
        ListNode()
                : m_prev(OutOfListMarker), m_next(nullptr)
        {}

        ListNode(const TKey& key)
                : m_key(key), m_prev(OutOfListMarker), m_next(nullptr)
        {}

        TKey m_key;
        ListNode* m_prev;
        ListNode* m_next;

        bool isInList() const {
            return m_prev != OutOfListMarker;
        }
    };

    static ListNode* const OutOfListMarker;

    /**
     * The value that we store in the hashtable. The list node is allocated from
     * an internal object_pool. The ListNode* is owned by the list.
     */
    struct HashMapValue {
        HashMapValue()
                : m_listNode(nullptr)
        {}

        HashMapValue(const TValue& value, ListNode* node)
                : m_value(value), m_listNode(node)
        {}

        TValue m_value;
        ListNode* m_listNode;
    };

    typedef tbb::concurrent_hash_map<TKey, HashMapValue, THash> HashMap;
    typedef typename HashMap::const_accessor HashMapConstAccessor;
    typedef typename HashMap::accessor HashMapAccessor;
    typedef typename HashMap::value_type HashMapValuePair;
    typedef std::pair<const TKey, TValue> SnapshotValue;

public:
    /**
     * The proxy object for TBB::CHM::const_accessor. Provides direct access to
     * the user's value by dereferencing, thus hiding our implementation
     * details.
     */
    struct ConstAccessor {
        ConstAccessor() {}

        const TValue& operator*() const {
            return *get();
        }

        const TValue* operator->() const {
            return get();
        }

        const TValue* get() const {
            return &m_hashAccessor->second.m_value;
        }

        bool empty() const {
            return m_hashAccessor.empty();
        }

    private:
        friend class ThreadSafeLRUCache;
        HashMapConstAccessor m_hashAccessor;
    };

    struct Accessor {
        Accessor() {}

        TValue& operator*() {
            return *get();
        }

        TValue* operator->() {
            return get();
        }

        TValue* get() {
            return &m_hashAccessor->second.m_value;
        }

        bool empty() const {
            return m_hashAccessor.empty();
        }

    private:
        friend class ThreadSafeLRUCache;
        HashMapAccessor m_hashAccessor;
    };

    

    /**
     * Create a container with a given maximum size
     */
    explicit ThreadSafeLRUCache(size_t maxSize);

    ThreadSafeLRUCache(const ThreadSafeLRUCache& other) = delete;
    ThreadSafeLRUCache& operator=(const ThreadSafeLRUCache&) = delete;

    ~ThreadSafeLRUCache() {
        clear();
    }

    /**
     * Find a value by key, and return it by filling the ConstAccessor, which
     * can be default-constructed. Returns true if the element was found, false
     * otherwise. Updates the eviction list, making the element the
     * most-recently used.
     */
    bool find(ConstAccessor& ac, const TKey& key);
    bool find(Accessor& ac, const TKey& key);

    /**
     * Insert a value into the container. Both the key and value will be copied.
     * The new element will put into the eviction list as the most-recently
     * used.
     *
     * If there was already an element in the container with the same key, it
     * will not be updated, and false will be returned. Otherwise, true will be
     * returned.
     */
    std::pair<bool, TValue> insert(const TKey& key, const TValue& value);

    /**
            modified insert for my purposes
     */
    std::pair<bool, TValue> insert(const TKey& key, const TValue& value, unsigned int hash);
    
    /**
     * Clear the container. NOT THREAD SAFE -- do not use while other threads
     * are accessing the container.
     */
    void clear();

    /**
     * Get a snapshot of the keys in the container by copying them into the
     * supplied vector. This will block inserts and prevent LRU updates while it
     * completes. The keys will be inserted in order from most-recently used to
     * least-recently used.
     */
    void snapshotKeys(std::vector<TKey>& keys);

    /**
     * Get the approximate size of the container. May be slightly too low when
     * insertion is in progress.
     */
    size_t size() const {
        return m_size.load();
    }

private:
    /**
     * Unlink a node from the list. The caller must lock the list mutex while
     * this is called.
     */
    void delink(ListNode* node);

    /**
     * Add a new node to the list in the most-recently used position. The caller
     * must lock the list mutex while this is called.
     */
    void pushFront(ListNode* node);

    /**
     * Evict the least-recently used item from the container. This function does
     * its own locking.
     */
    std::pair<TKey, TValue> evict();

    /**m_shards[i]
     * The maximum number of elements in the container.
     */
    size_t m_maxSize;

    /**
     * This atomic variable is used to signal to all threads whether or not
     * eviction should be done on insert. It is approximately equal to the
     * number of elements in the container.
     */
    std::atomic<size_t> m_size;

    /**
     * The underlying TBB hash map.
     */
    HashMap m_map;

    /**
     * The linked list. The "head" is the most-recently used node, and the
     * "tail" is the least-recently used node. The list mutex must be held
     * during both read and write.
     */
    ListNode m_head;
    ListNode m_tail;
    typedef std::mutex ListMutex;
    ListMutex m_listMutex;
};

template <class TKey, class TValue, class THash>
typename ThreadSafeLRUCache<TKey, TValue, THash>::ListNode* const
        ThreadSafeLRUCache<TKey, TValue, THash>::OutOfListMarker = (ListNode*)-1;

template <class TKey, class TValue, class THash>
ThreadSafeLRUCache<TKey, TValue, THash>::ThreadSafeLRUCache(size_t maxSize)
        : m_maxSize(maxSize), m_size(0),
          m_map(std::thread::hardware_concurrency() * 4) // it will automatically grow
{
    m_head.m_prev = nullptr;
    m_head.m_next = &m_tail;
    m_tail.m_prev = &m_head;
}

template <class TKey, class TValue, class THash>
bool ThreadSafeLRUCache<TKey, TValue, THash>::
find(ConstAccessor& ac, const TKey& key) {
    HashMapConstAccessor& hashAccessor = ac.m_hashAccessor;
    if (!m_map.find(hashAccessor, key)) {
        return false;
    }

    // Acquire the lock, but don't block if it is already held
    std::unique_lock<ListMutex> lock(m_listMutex, std::try_to_lock);
    if (lock) {
        ListNode* node = hashAccessor->second.m_listNode;
        // The list node may be out of the list if it is in the process of being
        // inserted or evicted. Doing this check allows us to lock the list for
        // shorter periods of time.
        if (node->isInList()) {
            delink(node);
            pushFront(node);
        }
        lock.unlock();
    }
    return true;
}

template <class TKey, class TValue, class THash>
bool ThreadSafeLRUCache<TKey, TValue, THash>::
find(Accessor& ac, const TKey& key) {
    HashMapConstAccessor& hashAccessor = ac.m_hashAccessor;
    if (!m_map.find(hashAccessor, key)) {
        return false;
    }

    // Acquire the lock, but don't block if it is already held
    std::unique_lock<ListMutex> lock(m_listMutex, std::try_to_lock);
    if (lock) {
        ListNode* node = hashAccessor->second.m_listNode;
        // The list node may be out of the list if it is in the process of being
        // inserted or evicted. Doing this check allows us to lock the list for
        // shorter periods of time.
        if (node->isInList()) {
            delink(node);
            pushFront(node);
        }
        lock.unlock();
    }
    return true;
}


template <class TKey, class TValue, class THash>
std::pair<bool, TValue> ThreadSafeLRUCache<TKey, TValue, THash>::
insert(const TKey& key, const TValue& value, unsigned int hash){
     // Insert into the CHM
     ListNode* node = new ListNode(key);
     HashMapAccessor hashAccessor;
     HashMapValuePair hashMapValue(key, HashMapValue(value, node));
     if (!m_map.insert(hashAccessor, hashMapValue)) {
         delete node;
         return make_pair(false, hashAccessor->second.m_value);
     }

     // Evict if necessary, now that we know the hashmap insertion was successful.
     size_t size = m_size.load();
     bool evictionDone = false;
     if (size >= m_maxSize) {
         // The container is at (or over) capacity, so eviction needs to be done.
         // Do not decrement m_size, since that would cause other threads to
         // inappropriately omit eviction during their own inserts.
         auto p = evict();
         evictionDone = true;
     }

     // Note that we have to update the LRU list before we increment m_size, so
     // that other threads don't attempt to evict list items before they even
     // exist.
     std::unique_lock<ListMutex> lock(m_listMutex);
     pushFront(node);
     lock.unlock();
     if (!evictionDone) {
         size = m_size++;
     }
     if (size > m_maxSize) {
         // It is possible for the size to temporarily exceed the maximum if there is
         // a heavy insert() load, once only as the cache fills. In this situation,
         // we have to be careful not to have every thread simultaneously attempt to
         // evict the extra entries, since we could end up underfilled. Instead we do
         // a compare-and-exchange to acquire an exclusive right to reduce the size
         // to a particular value.
         //
         // We could continue to evict in a loop, but if there are a lot of threads
         // here at the same time, that could lead to spinning. So we will just evict
         // one extra element per insert() until the overfill is rectified.
         if (m_size.compare_exchange_strong(size, size - 1)) {
             auto p=evict();
         }
     }
     value->hash=hash;
     return make_pair(true, value);
}


template <class TKey, class TValue, class THash>
std::pair<bool, TValue> ThreadSafeLRUCache<TKey, TValue, THash>::
insert(const TKey& key, const TValue& value) {
    // Insert into the CHM
    ListNode* node = new ListNode(key);
    HashMapAccessor hashAccessor;
    HashMapValuePair hashMapValue(key, HashMapValue(value, node));
    if (!m_map.insert(hashAccessor, hashMapValue)) {
        delete node;
        return std::make_pair(false, hashAccessor->second.m_value);
    }

    // Evict if necessary, now that we know the hashmap insertion was successful.
    size_t size = m_size.load();
    bool evictionDone = false;
    if (size >= m_maxSize) {
        // The container is at (or over) capacity, so eviction needs to be done.
        // Do not decrement m_size, since that would cause other threads to
        // inappropriately omit eviction during their own inserts.
        auto p = evict();
        evictionDone = true;
    }

    // Note that we have to update the LRU list before we increment m_size, so
    // that other threads don't attempt to evict list items before they even
    // exist.
    std::unique_lock<ListMutex> lock(m_listMutex);
    pushFront(node);
    lock.unlock();
    if (!evictionDone) {
        size = m_size++;
    }
    if (size > m_maxSize) {
        // It is possible for the size to temporarily exceed the maximum if there is
        // a heavy insert() load, once only as the cache fills. In this situation,
        // we have to be careful not to have every thread simultaneously attempt to
        // evict the extra entries, since we could end up underfilled. Instead we do
        // a compare-and-exchange to acquire an exclusive right to reduce the size
        // to a particular value.
        //
        // We could continue to evict in a loop, but if there are a lot of threads
        // here at the same time, that could lead to spinning. So we will just evict
        // one extra element per insert() until the overfill is rectified.
        if (m_size.compare_exchange_strong(size, size - 1)) {
            auto p=evict();
        }
    }
    return std::make_pair(true, value);
}

template <class TKey, class TValue, class THash>
void ThreadSafeLRUCache<TKey, TValue, THash>::
clear() {
    m_map.clear();
    ListNode* node = m_head.m_next;
    ListNode* next;
    while (node != &m_tail) {
        next = node->m_next;
        delete node;
        node = next;
    }
    m_head.m_next = &m_tail;
    m_tail.m_prev = &m_head;
    m_size = 0;
}

template <class TKey, class TValue, class THash>
void ThreadSafeLRUCache<TKey, TValue, THash>::
snapshotKeys(std::vector<TKey>& keys) {
    keys.reserve(keys.size() + m_size.load());
    std::lock_guard<ListMutex> lock(m_listMutex);
    for (ListNode* node = m_head.m_next; node != &m_tail; node = node->m_next) {
        keys.push_back(node->m_key);
    }
}

template <class TKey, class TValue, class THash>
inline void ThreadSafeLRUCache<TKey, TValue, THash>::
delink(ListNode* node) {
    ListNode* prev = node->m_prev;
    ListNode* next = node->m_next;
    prev->m_next = next;
    next->m_prev = prev;
    node->m_prev = OutOfListMarker;
}

template <class TKey, class TValue, class THash>
inline void ThreadSafeLRUCache<TKey, TValue, THash>::
pushFront(ListNode* node) {
    ListNode* oldRealHead = m_head.m_next;
    node->m_prev = &m_head;
    node->m_next = oldRealHead;
    oldRealHead->m_prev = node;
    m_head.m_next = node;
}

template <class TKey, class TValue, class THash>
std::pair<TKey, TValue> ThreadSafeLRUCache<TKey, TValue, THash>::
evict() {
    std::unique_lock<ListMutex> lock(m_listMutex);
    ListNode* moribund = m_tail.m_prev;
    if (moribund == &m_head) {
        // List is empty, can't evict
        TKey key;
        TValue val;
        return std::make_pair(key,val);
    }
    delink(moribund);
    lock.unlock();

    HashMapAccessor hashAccessor;
    if (!m_map.find(hashAccessor, moribund->m_key)) {
        // Presumably unreachable
        TKey key;
        TValue val;
        return std::make_pair(key,val);
    }
    std::pair<TKey, TValue> returnValue= std::make_pair(moribund->m_key,hashAccessor->second.m_value);
    m_map.erase(hashAccessor);
    delete moribund;
    return returnValue;
}

template <class TKey, class TValue, class THash = tbb::tbb_hash_compare<TKey>>
struct ThreadSafeScalableCache {
    using Shard = ThreadSafeLRUCache<TKey, TValue, THash>;
public:
    typedef typename Shard::ConstAccessor ConstAccessor;
    typedef typename Shard::Accessor Accessor;

    /**
     * Constructor
     *   - maxSize: the maximum number of items in the container
     *   - numShards: the number of child containers. If this is zero, the
     *     "hardware concurrency" will be used (typically the logical processor
     *     count).
     */
    explicit ThreadSafeScalableCache(size_t maxSize, size_t numShards = 0);

    ThreadSafeScalableCache(const ThreadSafeScalableCache&) = delete;
    ThreadSafeScalableCache& operator=(const ThreadSafeScalableCache&) = delete;

    /**
     * Find a value by key, and return it by filling the ConstAccessor, which
     * can be default-constructed. Returns true if the element was found, false
     * otherwise. Updates the eviction list, making the element the
     * most-recently used.
     */
    bool find(ConstAccessor& ac, const TKey& key);
    bool find(Accessor& ac, const TKey& key);


    /**
     * Insert a value into the container. Both the key and value will be copied.
     * The new element will put into the eviction list as the most-recently
     * used.
     *
     * If there was already an element in the container with the same key, it
     * will not be updated, and false will be returned. Otherwise, true will be
     * returned.
     */
    std::pair<bool, TValue> insert(const TKey& key, const TValue& value);

    /**
            modified insert
     */
    std::pair<bool, TValue> insert(const TKey& key, const TValue& value, unsigned int hash);
    
    /**
     * Clear the container. NOT THREAD SAFE -- do not use while other threads
     * are accessing the container.
     */
    void clear();

    /**
     * Get a snapshot of the keys in the container by copying them into the
     * supplied vector. This will block inserts and prevent LRU updates while it
     * completes. The keys will be inserted in a random order.
     */
    void snapshotKeys(std::vector<TKey>& keys);

    /**
     * Get the approximate size of the container. May be slightly too low when
     * insertion is in progress.
     */
    size_t size() const;

private:
    /**
     * Get the child container for a given key
     */
    Shard& getShard(const TKey& key);

    /**
     * The maximum number of elements in the container.
     */
    size_t m_maxSize;

    /**
     * The child containers
     */
    size_t m_numShards;
    typedef std::shared_ptr<Shard> ShardPtr;
    std::vector<ShardPtr> m_shards;
};

/**
 * A specialisation of ThreadSafeScalableCache providing a cache with efficient
 * string keys.
 */
template <class TValue>
using ThreadSafeStringCache = ThreadSafeScalableCache<
        ThreadSafeStringKey, TValue, ThreadSafeStringKey::HashCompare>;

template <class TKey, class TValue, class THash>
ThreadSafeScalableCache<TKey, TValue, THash>::
ThreadSafeScalableCache(size_t maxSize, size_t numShards)
        : m_maxSize(maxSize), m_numShards(numShards)
{
    if (m_numShards == 0) {
        m_numShards = std::thread::hardware_concurrency();
    }
    for (size_t i = 0; i < m_numShards; i++) {
        size_t s = maxSize / m_numShards;
        if (i == 0) {
            s += maxSize % m_numShards;
        }
        m_shards.emplace_back(std::make_shared<Shard>(s));
    }
}

template <class TKey, class TValue, class THash>
typename ThreadSafeScalableCache<TKey, TValue, THash>::Shard&
ThreadSafeScalableCache<TKey, TValue, THash>::
getShard(const TKey& key) {
    THash hashObj;
    constexpr int shift = std::numeric_limits<size_t>::digits - 16;
    size_t h = (hashObj.hash(key) >> shift) % m_numShards;
    return *m_shards.at(h);
}

template <class TKey, class TValue, class THash>
bool ThreadSafeScalableCache<TKey, TValue, THash>::
find(ConstAccessor& ac, const TKey& key) {
    return getShard(key).find(ac, key);
}

template <class TKey, class TValue, class THash>
bool ThreadSafeScalableCache<TKey, TValue, THash>::
find(Accessor& ac, const TKey& key) {
    return getShard(key).find(ac, key);
}


template <class TKey, class TValue, class THash>
std::pair<bool, TValue> ThreadSafeScalableCache<TKey, TValue, THash>::
insert(const TKey& key, const TValue& value) {
    return getShard(key).insert(key, value);
}

template <class TKey, class TValue, class THash>
std::pair<bool, TValue> ThreadSafeScalableCache<TKey, TValue, THash>::
insert(const TKey& key, const TValue& value, unsigned int hash){
    return getShard(key).insert(key, value, hash);
}


template <class TKey, class TValue, class THash>
void ThreadSafeScalableCache<TKey, TValue, THash>::
clear() {
    for (size_t i = 0; i < m_numShards; i++) {
        m_shards[i]->clear();
    }
}

template <class TKey, class TValue, class THash>
void ThreadSafeScalableCache<TKey, TValue, THash>::
snapshotKeys(std::vector<TKey>& keys) {
    for (size_t i = 0; i < m_numShards; i++) {
        m_shards[i]->snapshotKeys(keys);
    }
}

template <class TKey, class TValue, class THash>
size_t ThreadSafeScalableCache<TKey, TValue, THash>::
size() const {
    size_t size=0;
    for (size_t i = 0; i < m_numShards; i++) {
        size += m_shards[i]->size();
    }
    return size;
}



#endif //BGPGEOPOLITICS_LRUCACHE_H
