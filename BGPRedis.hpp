//
//  BGPRedis.hpp
//  BGPGeopol
//
//  Created by Kave Salamatian on 20/09/2019.
//  Copyright © 2019 Kave Salamatian. All rights reserved.
//
//  BGPRedis.hpp
//  BGPGeo
//
//  Created by zhangxinyi on 2019/3/19.
//  Copyright © 2019年 zhangxinyi. All rights reserved.
//

#ifndef BGPRedis_hpp
#define BGPRedis_hpp

#include <iostream>
#include <string>
#include <stdio.h>
#include <thread>
#include <sw/redis++/redis++.h>
#include "BGPGeopolitics.h"
//#include "BGPTables.h"
//#include "BGPEvent.h"
#include "BlockingQueue.h"

using namespace sw::redis;

class BGPEvent;
class RIBTable;

class BGPRedis {
public:
    BGPRedis(BlockingCollection<BGPEvent *> *queue, Redis *_redis);
    ~BGPRedis();
    void run();
    void setSavingMode();
    void resetSavingMode();
private:
    unsigned int  cnt=0;
    BlockingCollection<BGPEvent *> *queue;
    Redis* _redis;
    bool savingMode=false;
};



class ShardedBGPRedis{
public:
    ShardedBGPRedis(string host, int basePort, int dbase, int numShards);
    ~ShardedBGPRedis();
    BlockingCollection<BGPEvent *> * getQueue(unsigned int hash);
    Redis *getRedis(unsigned int hash);
    void run();
    void add(BGPEvent *event);
    void setSavingMode();
    void resetSavingMode();
    void getPrefixes();
    void getASes();
    void getPaths();
    void getLinks();
    void getRoutingTable();
    void populate();
    pair<long,long> getPathsStat();
    pair<long,long> getRoutingStat();
    int getNumShards();
private:
    Redis *bgpRedisConnect(string host, int port, int dbase);
    int numShards, basePort, dbase;
    string host;
    vector<BlockingCollection<BGPEvent *> *> queues;
    vector<Redis *> _redisVect;
    vector<BGPRedis*> redisShards;
    vector<std::thread> threads;
};

#endif /* BGPRedis_hpp */
