//
// Created by Kave Salamatian on 2018-12-03.
//
#include <algorithm>
#include <random>
#include "BGPTables.h"
#include "BGPGeopolitics.h"
#include "BGPSource.h"
#include "cache.h"
#include "tbb/tbb.h"
#include <boost/algorithm/string.hpp>
#ifdef __linux
    #include <sys/prctl.h>
#endif

using namespace tbb;
using namespace std;

struct bgpstream_patricia_node {
    /* flag if this node used */
    u_int bit;
    
    /* who we are in patricia tree */
    bgpstream_pfx_t prefix;
    
    /* left and right children */
    bgpstream_patricia_node_t *l;
    bgpstream_patricia_node_t *r;
    
    /* parent node */
    bgpstream_patricia_node_t *parent;
    
    /* pointer to user data */
    void *user;
};


struct bgpstream_patricia_tree {
    
    /* IPv4 tree */
    bgpstream_patricia_node_t *head4;
    
    /* IPv6 tree */
    bgpstream_patricia_node_t *head6;
    
    /* Number of nodes per tree */
    uint64_t ipv4_active_nodes;
    uint64_t ipv6_active_nodes;
    
    /** Pointer to a function that destroys the user structure
     *  in the bgpstream_patricia_node_t structure */
    bgpstream_patricia_tree_destroy_user_t *node_user_destructor;
};

struct pathComp {
    bool operator() (const SPrefixPath lhs, const SPrefixPath rhs) const {
        if (lhs->getPeer() == rhs->getPeer()){
            return false;
        }
        if (lhs->getScore()>rhs->getScore()){
            return false;
        } else {
            if (lhs->getScore()<rhs->getScore()){
                return true;
            } else {
                for (int i=0; i<lhs->shortPathLength;i++){
                    if (lhs->shortPath[i]<rhs->shortPath[i])
                        return true;
                }
                return false;
            }
        }
    }
};


RIBTable::RIBTable(unsigned int time, unsigned int duration): basetime(time), windowtime(time), duration(duration){
    ribTrie = new Trie();
}

BGPMessage *RIBTable::update(BGPMessage *bgpMessage){
    bgpstream_pfx_t *pfx;
    Peer* peer;
    SAS dest;
    bool globalOutage=false;
    char collectorId =cache->collectors.find(bgpMessage->collector)->second;
    SPrefixPath path;
    unsigned int time, pathHash;
    BGPEvent *event;
    bool pathAdded=false, pathwithdrawn=false;
    Category cat;
    
    peer=bgpMessage->peer;
    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    pfx=  (bgpstream_pfx_t *)&bgpMessage->pfx;
    peer = bgpMessage->peer;
    time = bgpMessage->timestamp;
    RIBElement *ribElement = (RIBElement *)bgpMessage->trieElement;
    switch(bgpMessage->type) {
        case BGPSTREAM_ELEM_TYPE_RIB:{
            if (bgpMessage->setPath(time)){
                cache->numActivePath++;
                event=new BGPEvent(time, NEWPATH);
                bgpMessage->prefixPath->toRedis(event->map);
                event->map["HSH"]=to_myencoding(bgpMessage->prefixPath->hash);
                event->map["PSTR"]=bgpMessage->prefixPath->str();
                event->hash=bgpMessage->prefixPath->getPeer();
                cache->bgpRedis->add(event);
                cache->pathsBF.insert(bgpMessage->prefixPath->str());
                event = new BGPEvent(time, PATHACT);
                bgpMessage->prefixPath->toRedis(event->map);
                event->hash=bgpMessage->prefixPath->getPeer();
                cache->bgpRedis->add(event);
            }
            path = bgpMessage->prefixPath;
            event= new BGPEvent(time,PATHA);
            event->map["T"]=to_string(time);
            event->map["pathIDA"]=path->str();
            event->map["peer"]=to_myencoding(path->getPeer());
            event->map["pfxID"]=to_myencodingPref(pfx);
            event->map["pathHash"]=to_myencoding(path->hash);
            event->hash= std::hash<std::string>{}(event->map["pfxID"]+":"+ event->map["peer"]);
            cache->bgpRedis->add(event);
            path = bgpMessage->prefixPath;
            pathHash = bgpMessage->pathHash;
            dest= bgpMessage->dest;
            dest->update(pfx, time);
            break;
        }
        case BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT:{
            if(bgpMessage->setPath(time)){
                path = bgpMessage->prefixPath;
                cache->numActivePath++;
                event=new BGPEvent(time, NEWPATH);
                path->toRedis(event->map);
                event->map["HSH"]=to_myencoding(path->hash);
                event->map["PSTR"]=path->str();
                event->hash=path->getPeer();
                cache->bgpRedis->add(event);
                cache->pathsBF.insert(path->str());
            } else {
                path = bgpMessage->prefixPath;
                path->announcementNum++;
            }
            pathHash =path->hash;
            cat= ribElement->addPath(path, pathHash, time);
            switch(cat){
                case AADup:{
                    bgpMessage->category = AADup;
                    break;
                }
                case AADiff: {
                    pathAdded = true;
                    pathwithdrawn = true;// PreviousPath != NULL it is an implicit withdraw
                    bgpMessage->category = AADiff;
 //                   cache->routingFilter.insert(bgpMessage->pfxStr+":"+to_string(path->getPeer()));
                    break;
                }
                case None:{
                    pathAdded = true;
                    cache->asCache[path->getDest()]->update(pfx,time);
                    bgpMessage->category=None;
      //              cache->routingFilter.insert(bgpMessage->pfxStr+":"+to_string(path->getPeer()));
                    break;
                }
                default:{
                    break;
                }
            }
            break;
        }
        case BGPSTREAM_ELEM_TYPE_WITHDRAWAL: {
            auto ret = ribElement->erasePath(collectorId,peer->getAsn(), time);
            globalOutage = ret.first;
            switch(ret.second){
                case WWDup: {
                    bgpMessage->category = WWDup;
                    break;
                }
                case Withdrawn: {
                    pathwithdrawn = true; // PreviousPath != NULL it is an implicit withdraw
                    bgpMessage->category = Withdrawn; // implicit withdrawal and replacement with different
                    event= new BGPEvent(time,WITHDRAW);
                    event->map["pfxID"]=to_myencodingPref(&bgpMessage->pfx);
                    event->map["peer"]=to_myencoding(peer->getAsn());
                    event->hash= std::hash<std::string>{}(event->map["pfxID"]+":"+ event->map["peer"]);
                    cache->bgpRedis->add(event);
                    break;
                }
                default:{
                    break;
                }
            }
            if (globalOutage){
                    //There is a global prefix withdraw
            //        for (auto it= trieElement->asSet.begin(); it != trieElement->asSet.end();it++){
            //            if((*it)->activePrefixTrie->search(pfx).second != NULL){
            //                cout<<"ERROR"<<endl;
            //            }
            //        }

            }
            break;
        }
        default: {
            break;
        }
    }
    return bgpMessage;
    //updateEventTable(bgpMessage);
}



void RIBTable::save(BGPGraph* g, unsigned int time, unsigned int dumpDuration){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    vector<pair<unsigned int, Link *>> linkVect;
//    cout<<"FULL FEED PEERS"<<endl;
//    for(auto p:cache->peersMap){
//        if (p.second->isFullFeed){
//            cout<<p.second->asNum<<endl;
//        }
//    }
    windowtime +=duration;
    cache->makeGraph(g, time, dumpDuration);
}

long RIBTable::size_of(){
    long size=4+4*8;
    return size;
}



BGPTable::BGPTable(unsigned int time, unsigned int duration): basetime(time), windowtime(time), duration(duration){
    routingTrie = new Trie();
}



void BGPTable::save(BGPGraph* g, unsigned int time, unsigned int dumpDuration){
    boost::shared_lock<boost::shared_mutex> lock(mutex_);
//    cout<<"FULL FEED PEERS"<<endl;
//    for(auto p:cache->peersMap){
//        if (p.second->isFullFeed){
//            cout<<p.second->asNum<<endl;
//        }
//    }
    windowtime +=duration;
    cache->makeGraph(g, time, dumpDuration);
}

long BGPTable::size_of(){
    long size=4+4*8;
    return size;
}

void TableFlagger::run(){
    BGPMessage *bgpMessage;
    RIBElement *trieElement;
    BGPEvent *event;
    bgpstream_pfx_t *pfx;
    pair<bool, void *> ret;
#ifdef __linux
    prctl(PR_SET_NAME,"TABLEFLAGGER");
#endif
    while(true){
        if (infifo.try_take(bgpMessage, std::chrono::milliseconds(1200000))==BlockingCollectionStatus::TimedOut){
            cout<< "Data Famine Table"<<endl;
            break;
        } else {
            if (bgpMessage->category != STOP)  {
                pfx = (bgpstream_pfx_t *)&(bgpMessage->pfx);
                ret =bgpTable->ribTrie->checkinsert(pfx);
                if (ret.first){
                    trieElement = (RIBElement *) ret.second;
                    event = new BGPEvent(bgpMessage->timestamp, NEWPREFIX);
                    event->map["PFX"]=bgpMessage->pfxStr;
                    event->map["PID"]=to_myencodingPref(&bgpMessage->pfx);
                    event->hash= std::hash<std::string>{}(event->map["pfxID"]);
                    cache->bgpRedis->add(event);
                } else {
                    trieElement=(RIBElement *) ret.second;
                }
                bgpMessage->trieElement = trieElement;
                bgpMessage = bgpTable->update(bgpMessage);
                bgpSource->bgpMessagePool->returnBGPMessage(bgpMessage);
                outfifo.add(bgpMessage);
            } else {
                infifo.add(bgpMessage);
                BGPEvent *event = new BGPEvent(bgpMessage->timestamp, ENDE);
                event->hash= 0;
                cache->bgpRedis->add(event);
                outfifo.add(bgpMessage);
                SBGPAPI data= new BGPAPI(NULL,0);
                cache->toAPIbgpbiew.add(data);
                break;
            }
        }
    }
    cout<< "Table Processing end"<<endl;
}


RIBElement::RIBElement(bgpstream_pfx_t *inpfx) {
    bgpstream_pfx_copy(&pfx, inpfx);
    pfxStr=to_myencodingPref(&pfx);
}

SPrefixPath RIBElement::getPath(unsigned int hash, unsigned int peer, unsigned int timestamp) {
    Redis *_redis=cache->bgpRedis->getRedis(peer);
    auto str = _redis->hget("PATHS", to_myencoding(hash));
    if (str) {
        std::unordered_map<string, string> pathMap;
        SPrefixPath path = std::make_shared<PrefixPath>(*str);
        auto ret=cache->pathsMap.insert(hash, path, timestamp);
        if(ret.first){
            return ret.second;
        }
    }
    return NULL;
}


bool RIBElement::getRoutingEntry(bgpstream_pfx_t *pfx,unsigned int peer){
    string pfxStr=to_myencodingPref(pfx);
    string peerStr= to_myencoding(peer);
    string str=pfxStr+":"+peerStr;
    if(cache->routingBF.contains(str)){
        vector<string> vec, results(3);
        unsigned int hash=std::hash<std::string>{}(str);
        Redis *_redis=cache->bgpRedis->getRedis(hash);
        _redis->lrange("PRE:"+str,-1,-1, std::back_inserter(vec));
        if (vec.size()>0){
            cache->routingentries.cacheMissed();
            boost::split(results, vec[0], [](char c){return c == ':';});
            if (results[1]=="A"){
                auto p=cache->routingentries.insert(str,from_myencoding(results[0]));
                cache->routingBF.insert(str);
                return true;
            }
            return false;
        }
    }
    return false;
}



Category RIBElement::addPath(SPrefixPath prefixPath, unsigned int pathHash, unsigned int time){
    BGPEvent *event;
    unsigned int previousHash;
//    boost::upgrade_lock<boost::shared_mutex> lock(mutex_);
    ThreadSafeScalableCache<string,unsigned int>::Accessor accessor;
    
    char collector=prefixPath->collector;
    unsigned int peer=prefixPath->getPeer();
    
    if (collectorsSet.insert(collector)){
        //new collector
        visibleCollectorsNum++;
    }
    OutageCollectors.erase(collector);
    if (addAS(prefixPath->getDest())){
//        checkHijack(prefixPath->dest);
//TODO checkHijack
        cache->asCache[prefixPath->getDest()]->update(&pfx,time);
    }
    string str=pfxStr+":"+to_myencoding(peer);
    if (cache->routingentries.find(accessor,str)){
        // the routing entry is in the cache
        // the peer has already a path!
        unsigned int previousHash=*accessor;
        SPrefixPath previous=NULL;
        if (previousHash !=0){
            auto p=cache->pathsMap.find(previousHash);
            if (p.first){
                cache->pathsMap.idCacheMissed();
                previous=p.second;
    //            if (p.second->getDest()==prefixPath->getDest()){
    //                previous=p.second;
    //            }
            }
            if (!previous){
                // The previous Path for the peer is not in memory ask from redis
                previous=getPath(previousHash,peer, time);
            }
            if (previous){
                if (!previous->equal(prefixPath)) {
                    //path change for a peer
                    //implicit withdraw of previous path
                    //addition of the new
                    previous->AADiff++;
                    *accessor=pathHash;
                    if (prefixPath->addPrefix(time)) {
                        BGPEvent *event = new BGPEvent(time, PATHACT);
                        prefixPath->toRedis(event->map);
                        event->hash=prefixPath->getPeer();
                        cache->bgpRedis->add(event);
                        cache->numActivePath++;
                    }
                    event = new BGPEvent(time, PATHA);
                    event->map["T"]=to_string(time);
                    event->map["pathIDA"] = to_myencodingPath(prefixPath->shortPath, prefixPath->shortPathLength);
                    event->map["peer"] = to_myencoding(prefixPath->getPeer());
                    event->map["pfxID"] = to_myencodingPref(&pfx);
                    event->map["pathHash"]=to_myencoding(pathHash);
                    event->hash= std::hash<std::string>{}(event->map["pfxID"]+":"+ event->map["peer"]);
                    cache->bgpRedis->add(event);
                    return AADiff;
                } else {
                    prefixPath->AADup++;
                    return AADup;
                }
            }
        } else {
            //New visible peer
            visiblePeerNum++;
        }
        *accessor=pathHash;
        return None;
    } else {
        // CheckRedis
        if (!getRoutingEntry(&pfx, peer)){
            //New visible peer
            visiblePeerNum++;
            auto p=cache->routingentries.insert(str,pathHash);
            cache->routingBF.insert(str);
            if (p.first) {
                if (prefixPath->addPrefix(time)) {
                    BGPEvent *event = new BGPEvent(time, PATHACT);
                    prefixPath->toRedis(event->map);
                    event->hash=prefixPath->getPeer();
                    cache->bgpRedis->add(event);
                    cache->numActivePath++;
                }
                if (!cache->peersMap.find(peer).first){
                    Peer *peerP = new Peer(peer);
                    auto res=cache->peersMap.insert(make_pair(peer,peerP));
                    if (!res.first){
                        delete peerP;
                    }
                }
                cache->peersMap[peer]->addPref();
                event = new BGPEvent(time, PATHA);
                event->map["T"]=to_string(time);
                event->map["pathIDA"] = prefixPath->str();
                event->map["peer"] = to_myencoding(prefixPath->getPeer());
                event->map["pfxID"] = to_myencodingPref(&pfx);
                event->map["pathHash"]=to_myencoding(pathHash);
                event->hash= std::hash<std::string>{}(event->map["pfxID"]+":"+ event->map["peer"]);
                cache->bgpRedis->add(event);
                cTime= time;
                return None;
            }
        }
        cache->routingentries.find(accessor,str);
        previousHash=*accessor;
    }
    return None;
}

pair<bool, Category> RIBElement::erasePath(char collector, unsigned int peer, unsigned int time){

    ThreadSafeScalableCache<string,unsigned int>::Accessor accessor;
    // We have to remove all paths in the peer
    string str=pfxStr+":"+to_myencoding(peer);
    if(cache->routingentries.find(accessor,str)){
        if (*accessor ==0){
            //already withdrawn
            return make_pair(false, WWDup);
        }
        cTime = time;
        *accessor=0;
        visiblePeerNum--;
        if (checkGlobalOutage(time)) {
            globalOutage= true;
            return make_pair(true, Withdrawn);
        } else {
            return make_pair(false, Withdrawn);
        }
    } else {
        if (!getRoutingEntry(&pfx, peer)){
            //Not exist or already withdrawn
            return make_pair(false, WWDup);
        } else {
            cTime = time;
            cache->routingentries.find(accessor,str);
            *accessor =0;
            visiblePeerNum--;
            if (checkGlobalOutage(time)) {
                globalOutage= true;
                return make_pair(true, Withdrawn);
            } else {
                return make_pair(false, Withdrawn);
            }
        }
    }
    return make_pair(false,WWDup);
}

bool RIBElement::checkGlobalOutage(unsigned int time){
    if (visibleCollectorsNum==0){
        for(auto as:asSet)
            cache->asCache[as]->withdraw(&pfx, time);
        return true;
    } else{
        return false;
    }
}

bool RIBElement::addAS(unsigned int asn){
//    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    return asSet.insert(asn);
}

string RIBElement::str(){
    string pfxStr;
    char buffer[20];
    bgpstream_pfx_snprintf(buffer, 20, &pfx);
    pfxStr=string(buffer);
    return pfxStr;
}

long RIBElement::size_of(){
    long sum=0;
    return sum;
}


bgpstream_patricia_walk_cb_result_t pathProcess(const bgpstream_patricia_tree_t *pt,const bgpstream_patricia_node_t *node, void *data){
    RIBElement *rib=(RIBElement *)node->user;
    long *sum=(long *)data;
    *sum +=rib->size_of();
    return BGPSTREAM_PATRICIA_WALK_CONTINUE;
    
/*    SPrefixPath path=(SPrefixPath )data;
    unsigned int src,dst,tmp;
    string linkId;
    if ((node->prefix.address.version != BGPSTREAM_ADDR_VERSION_UNKNOWN)){
        path->dest->activePrefixTrie->insert(&node->prefix,NULL);
        for (int i=0;i<path->shortPathLength-1;i++){
            src = path->shortPath[i];
            dst = path->shortPath[i+1];
            if (src>dst){
                tmp = src;
                src = dst;
                dst = tmp;
            }
            linkId=to_string(src) + "|" + to_string(dst);
            link=cache->linksMap.find(linkId).second;
            
            path->linkVect[i]->prefixTrie->insert(&node->prefix, NULL);
        }
    }*/
}

Trie::Trie(){
    /* Create a Patricia Tree */
    pt = bgpstream_patricia_tree_create(NULL);
}

bool Trie::insert(bgpstream_pfx_t *pfx, void *data){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    long prevCount=prefixNum();
    bgpstream_patricia_node_t *node = bgpstream_patricia_tree_insert(pt,pfx);
    long nextCount=prefixNum();
    bgpstream_patricia_tree_set_user(pt, node, data);
    return (nextCount == prevCount);
}

pair<bool, void*> Trie::search(bgpstream_pfx_t *pfx){
    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    bgpstream_patricia_node_t *node = bgpstream_patricia_tree_search_exact(pt, pfx);
    if (node == NULL) {
        return make_pair(false,(void*) NULL);
    } else {
        return make_pair(true, bgpstream_patricia_tree_get_user(node));
    }
}

pair<bool, void*> Trie::checkinsert(bgpstream_pfx_t *pfx){
    boost::upgrade_lock<boost::upgrade_mutex> lock(mutex_);
    RIBElement *trieElement;
    bgpstream_patricia_node_t *node = bgpstream_patricia_tree_search_exact(pt, pfx);
    if (node ==NULL){
        trieElement= new RIBElement(pfx);
//        trieElement = NULL;
        boost::upgrade_to_unique_lock<boost::shared_mutex> writeLock(lock);
        bgpstream_patricia_node_t *node = bgpstream_patricia_tree_insert(pt,pfx);
        bgpstream_patricia_tree_set_user(pt, node, trieElement);
        return make_pair(true,trieElement);
    }
    return make_pair(false,bgpstream_patricia_tree_get_user(node));
}

bool Trie::remove(bgpstream_pfx_t *pfx){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    bgpstream_patricia_node_t *node;
    if ((node=bgpstream_patricia_tree_search_exact(pt, pfx))!=NULL){
        bgpstream_patricia_tree_remove_node(pt, node);
        return true;
    } else
        return false;
}

long Trie::prefixNum(){
    return bgpstream_patricia_prefix_count(pt, BGPSTREAM_ADDR_VERSION_IPV4)+bgpstream_patricia_prefix_count(pt, BGPSTREAM_ADDR_VERSION_IPV6);
}
long Trie::prefix24Num(){
    return bgpstream_patricia_tree_count_24subnets(pt)+bgpstream_patricia_tree_count_64subnets(pt);
}

void Trie::savePrefixes(SPrefixPath prefixPath){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
//    bgpstream_patricia_tree_walk(pt,pathProcess , prefixPath);
}
    

void Trie::clear(){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    bgpstream_patricia_tree_clear(pt);
}

long Trie::size_of(){
    long sum=0;
    bgpstream_patricia_tree_walk(pt,pathProcess, &sum);
    return sum;
}

TableFlagger::TableFlagger(PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>>
                           &infifo, BlockingCollection<BGPMessage *> &outfifo, RIBTable *bgpTable,
                           BGPSource *bgpSource, int version): infifo(infifo), outfifo(outfifo),
                           version(version),bgpSource(bgpSource), bgpTable(bgpTable){}


