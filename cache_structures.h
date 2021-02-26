//
// Created by Kavé on 29/11/2020.
//

#ifndef BGPGEOPOLITICS_CACHE_STRUCTURES_H
#define BGPGEOPOLITICS_CACHE_STRUCTURES_H
//#include <set>
#include <mutex>
#include <condition_variable>
#include <list>
#include <vector>
#include <boost/thread/shared_mutex.hpp>
#include <iostream>
#include <map>
#include "tbb/concurrent_unordered_set.h"
#include "tbb/concurrent_unordered_map.h"
#include "tbb/parallel_for.h"
#include "LruCache.h"
#include "bloom_filter.hpp"


using namespace std;
using namespace tbb;


template < typename T> class MyThreadSafeSet: public concurrent_unordered_set<T>{
public:
//    std::set<T, Compare> set;
    MyThreadSafeSet():concurrent_unordered_set<T>(){ }


    bool insert(const T& val){
        return concurrent_unordered_set<T>::insert(val).second;
    }

    long erase(const T& val){

        return concurrent_unordered_set<T>::unsafe_erase(val);
    }

    long size_of(){
        long sum=0;
        for_each(concurrent_unordered_set<T>::begin(), concurrent_unordered_set<T>::end(),[&] (T val) {
            sum += val->size_of();});
        return sum;
    }
};

template< typename KeyType,typename ValueType> class MyThreadSafeMap: public concurrent_unordered_map<KeyType, ValueType>{
public:
    MyThreadSafeMap():concurrent_unordered_map<KeyType, ValueType>(){ }

    pair<bool, ValueType> insert(pair<KeyType,ValueType> p){
        auto res=concurrent_unordered_map<KeyType, ValueType>::insert(p);
        return make_pair(res.second, res.first->second);
    }

/*    pair<bool, ValueType> checkinsert(pair<KeyType,ValueType> p){
        
        if (insert(p)){
            return make_pair(true, p.second);
        } else {
            return make_pair(false,find )
        }
        return insert(p);
    }*/

    bool erase(KeyType key){
        return concurrent_unordered_map<KeyType, ValueType>::unsafe_erase(key);
    }

    pair<bool, ValueType> find(const KeyType key){
        auto it=concurrent_unordered_map<KeyType, ValueType>::find(key);
        if (it == concurrent_unordered_map<KeyType, ValueType>::end()){
            return pair<bool, ValueType>(false, (ValueType)NULL);
        } else {
            return pair<bool, ValueType>(true, it->second);
        }
    }

    long size_of(){
        long sum=0;
        for_each(concurrent_unordered_map<KeyType, ValueType>::begin(), concurrent_unordered_map<KeyType, ValueType>::end(),[&] (pair<KeyType, ValueType> p) {
            sum += p.second->size_of();});
        return sum;
    }
};

template< typename ValueType, typename HashSize> class MyThreadSafeHashMap: public MyThreadSafeMap<HashSize,ValueType>{
public:
    int collision = 0;
    // default values recommended by http://isthe.com/chongo/tech/comp/fnv/
    const uint32_t Prime = 0x01000193; //   16777619
    const uint32_t Seed  = 0x811C9DC5; // 2166136261
    /// hash a single byte
    inline uint32_t fnv1A(unsigned char oneByte, uint32_t hasH) {
        return (oneByte ^ hasH) * Prime;
    }
    /// hash an std::string
    uint32_t fnv1A(const std::string& str){
        uint32_t hasH =Seed;
        const char* text = str.c_str();
        while (*text)
            hasH = fnv1A((unsigned char)*text++, hasH);
        return hasH ;
    }
    HashSize hash(string str){
        return fnv1A(str);
    }

    pair<bool, HashSize> checkinsert(ValueType& value){
//        boost::upgrade_lock<boost::shared_mutex> lock(mutex_);
        pair<bool, ValueType> p;
        bool found=false, coll= false;
        string str=value->str(),str1=str;
        HashSize h;
        int count =0;
        while (!found){
            h= hash(str1);
            p=MyThreadSafeMap<HashSize,ValueType>::find(h);
            if (!p.first)
                break;
            else {
                if (p.second->str()==str){
                    found = true;
                    break;
                } else {
                    str1=str+":"+to_string(count);
                    count++;
                    coll = true;
                }
            }
        }
        if (!found){
            if (coll){
                collision++;
            }
            if (checkinsert(make_pair(h,value))){
                return make_pair(true, h);
            }
        }
        return make_pair(false,h);
    }

    pair<bool, pair<HashSize,ValueType>> find(const string str){
       pair<bool, ValueType> p;
        bool found = false;
        string str1 = str;
        HashSize h;
        int count =0;
        while (!found){
            h = hash(str1);
            p =  MyThreadSafeMap<HashSize,ValueType>::find(h);
            if (!p.first)
                break;
            else {
                if (p.second->str()==str){
                    found = true;
                    break;
                } else {
                    str1=str+":"+to_string(count);
                    count++;
                }
            }
        }
        if (found){
            return make_pair(true,make_pair(h,p.second));
        } else {
            return make_pair(false,make_pair(h,(ValueType)NULL));
        }
    }

//    using MyThreadSafeMap<HashSize,ValueType>::checkinsert;
//    using MyThreadSafeMap<HashSize,ValueType>::operator[];
};



class MyThreadSafeBloomFilter{
private:
    bloom_parameters parameters;
    mutable boost::shared_mutex mutex_;
public:
    bloom_filter *filter;
    MyThreadSafeBloomFilter(int size, double faProb){
        parameters.projected_element_count = size;

        // Maximum tolerable false positive probability? (0,1)
        parameters.false_positive_probability = faProb; // 1 in 10000
        // Simple randomizer (optional)
        parameters.random_seed = 0xA5A5A5A5;
        parameters.compute_optimal_parameters();

        //Instantiate Bloom Filter
        filter =new bloom_filter(parameters);
    }

    template <typename T> inline void insert(const T& t){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        filter->insert(t);
    }

    template <typename T> inline bool checkinsert(const T& t){
        boost::upgrade_lock<boost::upgrade_mutex> lock(mutex_);
        if (filter->contains(t)){
            return false;
        } else {
            boost::upgrade_to_unique_lock<boost::shared_mutex> writeLock(lock);
            filter->insert(t);
            return true;
        }
    }

    template <typename T> inline bool contains(const T& t){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return filter->contains(t);
    }
};

class ThreadSafeScalableBF{

private:
    size_t m_numShards, bfSize;
    double faProb;
    typedef MyThreadSafeBloomFilter Shard;
    typedef std::shared_ptr<Shard> ShardPtr;

    std::vector<ShardPtr> m_shards;
public:
    ThreadSafeScalableBF(size_t bfSize, size_t numShards, double faProb):
    m_numShards(numShards),bfSize(bfSize){
        if (m_numShards == 0) {
            m_numShards = std::thread::hardware_concurrency();
        }
        size_t shardBfSize=bfSize / m_numShards;
        m_shards.emplace_back(std::make_shared<MyThreadSafeBloomFilter>(shardBfSize+ bfSize% shardBfSize,faProb));
        for (size_t i = 1; i < m_numShards; i++) {
            m_shards.emplace_back(std::make_shared<MyThreadSafeBloomFilter>(shardBfSize,faProb));
        }
    }
    
    template <typename T> Shard& getShard(const T& t) {
        hash<T> hash;
        size_t h = hash(t) % m_numShards;
        return *m_shards.at(h);
    }


    template <typename T> bool contains(const T& t){
        return getShard(t).contains(t);
    }

    template <typename T> inline void insert(const T& t){
        return getShard(t).insert(t);
    }
};


template<typename TKey, typename TValue> class MyThreadSafeScalableCache: ThreadSafeScalableCache<TKey, TValue>{

private:
    std::atomic<unsigned int> cacheMiss={0};
    std::atomic<unsigned int> cacheUse={0};
    
public:
    typedef typename ThreadSafeScalableCache<TKey, TValue>::ConstAccessor ConstAccessor;
    typedef typename ThreadSafeScalableCache<TKey, TValue>::Accessor Accessor;
    MyThreadSafeScalableCache(size_t maxSize, size_t numShards = 0):ThreadSafeScalableCache<TKey, TValue>(maxSize,numShards){
    }
    
    void cacheMissed(){
        cacheMiss++;
    }
    
    size_t getMissed(){
        return cacheMiss;
    }
    std::pair<bool, TValue> insert(const TKey& key, const TValue& value){
        cacheUse++;
        auto ret =ThreadSafeScalableCache<TKey, TValue>::insert(key,value);
        return ret;
    }
    
    
    std::pair<bool, TValue> insert(const TKey& key, const TValue& value, unsigned int hash){
        cacheUse++;
        auto ret =ThreadSafeScalableCache<TKey, TValue>::insert(key,value,hash);
        return ret;
    }
    
    bool find(ConstAccessor& ac, const TKey& key){
        cacheUse++;
        return ThreadSafeScalableCache<TKey, TValue>::find(ac, key);
    }
    
    bool find(Accessor& ac, const TKey& key){
        cacheUse++;
        return ThreadSafeScalableCache<TKey, TValue>::find(ac, key);
    }
    
    
    using ThreadSafeScalableCache<TKey, TValue>::size;
};


typedef unsigned int HashType;

template< typename ValueType>  class MyScalableLRUHashCache{
private:

    /**
     * The child containers
     */
    size_t m_numShards;
    
    typedef typename MyThreadSafeScalableCache<HashType, ValueType>::ConstAccessor IdAccessor;
    typedef typename MyThreadSafeScalableCache<string, ValueType>::ConstAccessor StrAccessor;

    MyThreadSafeScalableCache<HashType,ValueType> idCache;
    MyThreadSafeScalableCache<string, ValueType> strCache;
//    ThreadSafeScalableBF dataBFCache;
    std::atomic<unsigned int> cacheMiss={0};
    std::atomic<unsigned int> cacheUse={0};
    std::atomic<unsigned int> globalCount={1};
    size_t capacity;
    using iterator= typename concurrent_unordered_map<HashType, ValueType>::iterator ;
public:

    MyScalableLRUHashCache(size_t capacity,size_t numShards):m_numShards(numShards), capacity(capacity), strCache(capacity,numShards), idCache(capacity,numShards)  {
    }
    

//    bool checkBF(string str){
//        return dataBFCache.contains(str);
//    }

//    void insertBF(string str){
//        dataBFCache.insert(str);
//    }

    pair<bool, ValueType> find(string str){
        cacheUse++;
        StrAccessor ac;
        ValueType val=NULL;
        if (strCache.find(ac,str)) {
            return(make_pair(true,*ac));
        }
        return (make_pair(false, val));
    }

    pair<bool, ValueType> find(HashType h) {
        cacheUse++;
        IdAccessor ac;
        ValueType val=NULL;
        if (idCache.find(ac,h)) {
            return(make_pair(true,*ac));
        }
        return (make_pair(false, val));
    }
    
    pair<bool,ValueType> insert(ValueType &value, unsigned int timestamp){
        cacheUse++;
        HashType hash=getID();
        auto ret=strCache.insert(value->sstr(),value,hash);
        if (!ret.first) {
            auto ret1=idCache.insert(ret.second->hash,ret.second);
            return make_pair(true,ret.second);
        } else {
            auto ret1=idCache.insert(value->hash,value);
            if (ret1.first){
                return make_pair(true, ret1.second);
            }
            return make_pair(false,ret1.second);
        }
    }

    pair<bool,ValueType> insert(HashType &hash, ValueType &value, unsigned int timestamp){
        cacheUse++;
        auto ret=idCache.insert(hash,value);
        if (ret.first){
            auto ret1=strCache.insert(value->sstr(),value);
            return make_pair(true, ret.second);
        }
        return make_pair(false, ret.second);
    }

    int getCapacity() {
        return capacity;
    }

    int size(){
        return idCache.size();

    }

    size_t  cacheMissed(){
        cacheMiss++;
        return cacheMiss;
    }
    
    size_t strCacheMissed(){
        strCache.cacheMissed();
        cacheMissed();
        return strCache.getMissed();
    }
    
    size_t idCacheMissed(){
        idCache.cacheMissed();
        cacheMissed();
        return idCache.getMissed();
    }

    
    void setID(HashType val){
        globalCount=val;
    }

private:
    HashType getHash(string str){
        uint64_t h128[2];
        MurmurHash3::hash128<false>(str.data(), str.size(), 0, h128);
        HashType hash=h128[0];
        return hash;
    }
    
    HashType getID(){
//        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        globalCount++;
        return (HashType)(globalCount);
    }
};






#endif //BGPGEOPOLITICS_CACHE_STRUCTURES_H
