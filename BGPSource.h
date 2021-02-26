//
// Created by Kave Salamatian on 17/11/2018.
//


#ifndef BGPGEOPOLITICS_BGPSTREAM_H
#define BGPGEOPOLITICS_BGPSTREAM_H

//#include "cache.h"
#include "BGPGeopolitics.h"
#include "BlockingQueue.h"
#include <list>
#include <vector>

extern BGPCache *cache;
class BGPCache;
class BGPMessage;
class BGPMessageComparer;


class BGPMessagePool{
public:
    BlockingCollection<BGPMessage *> bgpMessages;
    long count = 0;
    int capacity;


    BGPMessagePool(int capacity);
    BGPMessage* getBGPMessage(long order, bgpstream_elem_t *elem, unsigned int time, std::string collector);
    void returnBGPMessage(BGPMessage* bgpMessage);
private:
    bool isPoolAvailable();
};


class Trie;

class BGPSource {
public:
    int mode = 0;
    int count = 0;
    //    concurrent_hash_map<unsigned int, BlockingCollection<BGPMessage *> *> inProcess;
    Trie *inProcess;
    PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> &fifoQueue;
    map<std::string, unsigned short int> &collectors;
    unsigned int t_start, t_end, dumpDuration;
    int version;
    BGPMessagePool *bgpMessagePool;


    BGPSource(BGPMessagePool *bgpMessagePool,PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> &fifo, unsigned int t_start, unsigned int t_end, unsigned int dumpDuration, std::map<std::string, unsigned short int> &collectors, std::string &captype, int version);

    int run();
    void returnBGPMessage(BGPMessage* bgpMessage);
private:
};


#endif //BGPGEOPOLITICS_BGPSTREAM_H
