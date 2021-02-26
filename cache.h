//
// Created by Kave Salamatian on 25/11/2018.
//

#ifndef BGPGEOPOLITICS_CACHE_H
#define BGPGEOPOLITICS_CACHE_H
#include <sqlite3.h>
#include <set>
#include <map>
#include <thread>
#include <limits>
#include "BGPGraph.h"
#include "BGPGeopolitics.h"

//#include "BGPTables.h"
#include "BGPEvent.h"
#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_unordered_set.h"
#include "tbb/concurrent_queue.h"
#include "BlockingQueue.h"
#include "apibgpview.h"
#include "BGPRedis.hpp"
#include "json.hpp"

//using namespace boost;

#define probFA 0.05

class RoutingTable;
class AS;
class Link;
class Path;
class BGPMessage;

using namespace boost;
using namespace std;
using namespace tbb;
using json = nlohmann::json;

enum Status{CONNECTED=1, DISCONNECTED=2, OUTAGE=4, HIJACKED=8, HIJACKING=16,BOGUS=32};
extern sqlite3 *db;

class Trie;
class BGPGraph;

class Prefix{
public:
    string prefStr;
    Prefix(string str):prefStr(str){}
    string str(){
        return prefStr;
    }
};


class Peer{
protected:
    unsigned int asNum;
    unsigned int prefNum = 0;
    unsigned int FullFeedThresh=400000;
    bool isFullFeed = false;
public:
//    unsigned short int collectorNum;

    Peer(unsigned int asNum): asNum(asNum){}
    string str(){
        return to_string(asNum);
    }
    
    bool addPref(){
        prefNum ++;
        if (prefNum>FullFeedThresh)
            isFullFeed = true;
        return isFullFeed;
    }
    unsigned int getAsn(){
        return asNum;
    }
};


class semaphore
{
    //The current semaphore count.
    char count_;

    //mutex_ protects count_.
    //Any code that reads or writes the count_ data must hold a lock on
    //the mutex.
    boost::mutex mutex_;

    //Code that increments count_ must notify the condition variable.
    boost::condition_variable condition_;

public:
    explicit semaphore()
    : count_(1),
    mutex_(),
    condition_()
    {
    }

    unsigned int get_count() //for debugging/testing only
    {
        //The "lock" object locks the mutex when it's constructed,
        //and unlocks it when it's destroyed.
        boost::unique_lock<boost::mutex> lock(mutex_);
        return count_;
    }

    void release() //called "release" in Java
    {
        boost::unique_lock<boost::mutex> lock(mutex_);

        ++count_;

        //Wake up any waiting threads.
        //Always do this, even if count_ wasn't 0 on entry.
        //Otherwise, we might not wake up enough waiting threads if we
        //get a number of signal() calls in a row.
        condition_.notify_one();
    }

    void acquire() //called "acquire" in Java
    {
        boost::unique_lock<boost::mutex> lock(mutex_);
        while (count_ == 0)
        {
            condition_.wait(lock);
        }
        --count_;
    }
};

enum BugType {PREFIXBUG=0,PATHBUG=1,ASBUG=2,LINKBUG=3};
class Bug{
private:
    BugType type;
    unsigned int hash;
public:
    Bug(BugType type, unsigned int hash): type(type), hash(hash){};
};

class AS {
protected:
    unsigned int asNum;
    string name="??";
    string country="??";
    string RIR="??";
    string ownerAddress= "??";
    string lastChanged = "??";
    string description = "??";
    string dateCreated = "??";
    double risk=0.0, geoRisk=0.0, secuRisk=0.0,  otherRisk=0.0;
    unsigned int activePrefixNum=0;
    unsigned int allPrefixNum=0;
    unsigned int activePrefix24Num=0;
    unsigned int allPrefix24Num=0;
    unsigned int activePathsNum=0;
    unsigned int degree=0;
    MyThreadSafeSet<unsigned long> links;
    Trie *activePrefixTrie;
    Trie *inactivePrefixTrie;
    unsigned int cTime=0;
    bool observed=false;
    int status=0;
    bool outage=false;
    boost::graph_traits<Graph>::vertex_descriptor vertex=-1;
    bool touched = false;
private:
    mutable boost::shared_mutex mutex_;
public:
    AS(int asn);
    AS(int asn, string inname, string incountry, string inRIR, float risk);
    AS(std::unordered_map<string, string> asMap);
    ~AS();
    void updateInfo(std::unordered_map<string, string> asMap);
    void updateJSON(json &j);
    void updateBGPView();
    void insertDB();
    void updateDB();
    int getStatus();
    unsigned int getNum();
    string getName();
    int size_of();
    boost::graph_traits<Graph>::vertex_descriptor checkVertex(BGPGraph *bgpg);
    void removeVertex(BGPGraph *bgpg);
    void clearLinks();
    bool checkLink(unsigned long linkHash);
    bool hasLinks();
    bool checkOutage();
    Link* findLink(unsigned long linkHash, BGPCache *cache);
    bool update(bgpstream_pfx_t *pfx, unsigned int time);
    void addLink(unsigned long linkHash, unsigned int time);
    void removeLink(unsigned long linkHash, unsigned int time);
    bool withdraw(bgpstream_pfx_t *pfx,unsigned int time);
    double fusionRisks();
    void toMap(std::unordered_map<std::string, std::string> &map);
    void fromRedis(string str);
    void setObserved();
    bool isObserved();
    void touch();
    void untouch();
    bool isTouched();
    boost::graph_traits<Graph>::vertex_descriptor getVertex();
};

typedef std::shared_ptr<AS> SAS;

class Link {
protected:
    unsigned int src;
    unsigned int dst;
    SAS srcAS;
    SAS dstAS;
    unsigned int bTime; // First time seen
    unsigned int cTime; //last Change Time
    bool active = false;
    int pathNum=0;
//    boost::graph_traits<Graph>::edge _descriptor edge;
    bool touched = false;
private:
    semaphore sem;
//    mutable boost::shared_mutex mutex_;
public:
    Link(unsigned int src, unsigned int dst, unsigned int time);
    Link(std::unordered_map<std::string, std::string> map);
    void update(std::unordered_map<std::string, std::string> map);
    string str();
    int size_of();
    void addPath(unsigned int time);
    void withdraw(unsigned int time);
    void toRedis(std::unordered_map<std::string, std::string> &map);
    void fromRedis(string str);
    unsigned long linkID();
    void checkEdge(BGPGraph *bgpg);
    void removeEdge(BGPGraph *bgpg);
    bool isActive();
    void setActive(unsigned int time);
    void unActive(unsigned int time);
    bool isTouched();
    void touch();
    void unTouch();
    void addLinks(unsigned int time);
    void removeLinks(unsigned int time);
    
};


class PrefixPath;
typedef std::shared_ptr<PrefixPath> SPrefixPath;

class PrefixPath {
public:
    unsigned int hash=0;
    char pathLength;
    char shortPathLength;
    unsigned int *shortPath;
    unsigned int prefNum = 0;
    short int collector;
    bool active = true;
    unsigned int lastChange = 0; //last Change time
    unsigned int lastActive = 0; // last active time
    int announcementNum = 0;
    short int AADiff=0, AADup=0, WADup=0, WWDup=0, Flap=0, Withdraw=0;
    double globalRisk=0.0;
    double meanUp=0.0, meanDown=0.0;
    semaphore sem;

    PrefixPath(BGPMessage *bgpMessage, unsigned int time);
    PrefixPath(string str);
    PrefixPath(vector<unsigned int> &pathVect);
    void update(string str);
    PrefixPath();
    ~PrefixPath();
    void setHash(unsigned int h);
    double getScore() const;
    string str() const ;
    string sstr() const;
    int size_of();
    unsigned int getPeer() const;
    unsigned int getDest() const;
    bool addPrefix(unsigned int time);
    bool erasePrefix(unsigned int time);
    bool checkPrefix(bgpstream_pfx_t *pfx);
//    void addEvent(BGPEvent *bgpEvent, long order);
    bool equal(SPrefixPath path) const;
    void save(BGPCache *cache);
    void setPathActive(unsigned int time);
    void setPathNonActive(unsigned int time);
    void toRedis(std::unordered_map<std::string, std::string> &map);
    void fromRedis(string str);
    void saveToRedis(unsigned int timestamp);
};
typedef std::shared_ptr<PrefixPath> SPrefixPath;




class BGPEvent;
class BGPCache {
public:
    int numActivePath=0;
    unsigned int beginTime;
    map<string, unsigned short int> &collectors;
    ShardedBGPRedis *bgpRedis;
    BlockingCollection<BGPAPI *> toAPIbgpbiew;
    concurrent_queue<AS *> touchedASes;
    concurrent_queue<Link *> touchedLinks;

    MyThreadSafeMap<unsigned short int, Peer*> peersMap;
    MyThreadSafeMap<unsigned int, SAS> asCache;
    MyThreadSafeMap<unsigned long, Link *> linksMap;
    MyScalableLRUHashCache<SPrefixPath> pathsMap={500000,12};
    ThreadSafeScalableBF pathsBF={50000000,12,probFA};
    MyThreadSafeScalableCache<string,unsigned int> routingentries{20000000,12};
    ThreadSafeScalableBF routingBF={200000000,12,probFA};
    
    
    MyThreadSafeSet<Bug *> bogons;
    string ppath;
    BGPGraph *bgpg;
    semaphore sem;
    std::thread apiThread;

    BGPCache(string dbname, BGPGraph *g, ShardedBGPRedis *bgpRedis, map<string, unsigned short int> &collectors, unsigned int beginTime, string ppath): bgpg(g), bgpRedis(bgpRedis), collectors(collectors), beginTime(beginTime), ppath(ppath){
        if (sqlite3_open(dbname.c_str(), &db) != SQLITE_OK) {
            cout << "Can't open database: " << sqlite3_errmsg(db) << endl;
        }
        apibgpview = new APIbgpview(toAPIbgpbiew);
        apiThread = std::thread(&APIbgpview::run, apibgpview);
    }

    int fillASCache();
    void updateAS(SAS as, unsigned time);
    SAS chkAS(unsigned int asn, unsigned int time);
    Link *chkLink(unsigned int src, unsigned int dst, unsigned int time);
    long size_of();
    void makeGraph(BGPGraph* g, unsigned int time, unsigned int dumpDuration);
private:
    APIbgpview *apibgpview;
};




#endif //BGPGEOPOLITICS_CACHE_H
