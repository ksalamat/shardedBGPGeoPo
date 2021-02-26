//
// Created by Kave Salamatian on 2018-12-05.
//

#ifndef BGPGEOPOLITICS_BGPSAVER_H
#define BGPGEOPOLITICS_BGPSAVER_H

#include <boost/filesystem.hpp>
#include "json.hpp"
#include <iostream>
#include <fstream>
#include <chrono>
#include <tbb/parallel_for.h>
#include "bgpstream_utils_patricia.h"
#ifdef __linux
    #include <sys/prctl.h>
#endif

using namespace std;
using namespace boost::filesystem;
using json = nlohmann::json;
using namespace std::chrono;
using namespace tbb;

extern BGPCache *cache;

class Stats {
public:
    long numBGPmsgAll = 0, numBGPlastsec = 0, numUpdates = 0, numWithdraw = 0, numRIB = 0, numPathall = 0, numNewPathlastSec = 0,
        numPrefixall = 0, numPrefixlastsec = 0, numCollector = 0, streamQueuesize = 0, details = 0, numActivepaths = 0,
    numNewactivepaths = 0, numAS =0, numLink = 0, processTime =0, numInactivePath=0, numRoutingEntriesAll=0, numRoutingEntriesActive=0;
    double strPathCacheMiss=0.0, idPathCacheMiss=0.0, routingCacheMiss=0.0;
    
    unsigned int time;
    double delay = 0.0;
    RIBTable *table=NULL;
    BGPGraph *g=NULL;
    high_resolution_clock::time_point start=high_resolution_clock::now(),end;
    Stats(unsigned int time): time(time) {}
    Stats(RIBTable *bgptable): table(bgptable){
    }
    void fill(Stats& stats){
        time = stats.time;
        numBGPmsgAll =  stats.numBGPmsgAll;
        numBGPlastsec = stats.numBGPlastsec;
        numUpdates = stats.numUpdates;
        numWithdraw = stats.numWithdraw;
        numRIB = stats.numRIB;
        numPathall = stats.numPathall;
        numNewPathlastSec = stats.numNewPathlastSec;
        numPrefixall = stats.numPrefixall;
        numPrefixlastsec = stats.numPrefixlastsec;
        numCollector = stats.numCollector;
        streamQueuesize = stats.streamQueuesize;
        details = stats.details;
        numActivepaths= stats.numActivepaths;
        numNewactivepaths = stats.numNewactivepaths;
        processTime = stats.processTime;
        numInactivePath = stats.numInactivePath;
        numRoutingEntriesActive=stats.numRoutingEntriesActive;
        numRoutingEntriesAll=stats.numRoutingEntriesAll;
    }

    void update(BGPMessage* bgpMessage){
        numBGPmsgAll +=1;
        switch (bgpMessage->type ){
            case BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT:
                numUpdates +=1;
                break;
            case BGPSTREAM_ELEM_TYPE_WITHDRAWAL:
                numWithdraw +=1;
                break;
            case BGPSTREAM_ELEM_TYPE_RIB:
                numRIB +=1;
                break;
            default:
                break;
        }
    }


    void makeReport(Stats laststats, unsigned int inTime){
        duration<double, std::milli> processDuration;
        time = inTime;
        Redis *_redis = cache->bgpRedis->getRedis(max(1, cache->bgpRedis->getNumShards()-1));
        auto p=_redis->getPathsStat();
        numPathall = p.first;
        numActivepaths = p.second;
        numInactivePath = numPathall-numInactivePath;
        p=_redis->getRoutingStat();
        numRoutingEntriesActive=p.second;
        numRoutingEntriesAll=p.first+p.second;
        numAS= num_vertices(g->g);
        numLink = num_edges(g->g);
        numPrefixall = table->ribTrie->prefixNum();
        numBGPlastsec = numBGPmsgAll - laststats.numBGPmsgAll;
        numNewPathlastSec = numPathall - laststats.numPathall;
        numPrefixlastsec = numPrefixall - laststats.numPrefixall;
        numNewactivepaths = numActivepaths -  laststats.numActivepaths;
        end = high_resolution_clock::now();
        processDuration=(end-start);
        processTime= processDuration.count();
        delay=processTime*1.0 /numBGPlastsec*1000; //in usec
        start= end;
        strPathCacheMiss=cache->pathsMap.strCacheMissed()*1.0/
    }

    string toJson(string &str){
        json j,j1;
        j["processDelay"] = delay;
        j["processTime"] = processTime;
        j["numBGPmsgAll"] = numBGPmsgAll;
        j["numBGPlastsec"]=numBGPlastsec;
        j["numUpdates"]=numUpdates;
        j["numWithdraw"]=numWithdraw;
        j["numRIB"]=numRIB;
        j["numPathall"]=numPathall;
//        j["numNewPathlastSec"]=numNewPathlastSec;
        j["numPrefixall"]=numPrefixall;
//        j["numPrefixlastsec"]=numPrefixlastsec;
        j["numCollector"]=numCollector;
        j["numActivepaths"]=numActivepaths;
        j["numInactivepaths"]= numInactivePath;
//        j["numNewactivepaths"]=numNewactivepaths;
        j["numAS"]=numAS;
        j["numLink"]=numLink;
        j1[to_string(time)]=j;
        str = j1.dump();
        return str;
    }

    void printStr(){
        string str;
        cout<<toJson(str)<<endl;
    }
};


class ScheduleSaver{
public:
    ScheduleSaver(int start, int dumpDuration, BlockingCollection<BGPMessage *> &infifo,
            RIBTable *bgpTable, BlockingCollection<GraphToSave *> &graphsToSave, string p): time(start), dumpDuration(dumpDuration),
            infifo(infifo), lastStats(start), stats(start), table(bgpTable), dumpath(p), graphsToSave(graphsToSave){

//        string dumpath=p+"dumps";
        string dumpath=p;
        path pp=path(dumpath);
        stats.table = table;
        if (!exists(pp) || !is_directory(pp)) {
            cout<<pp<<endl;
            create_directory(pp);
        }
        perfFileName = dumpath+"/perf.dat";
    }
    

    void saveGraph(BGPGraph* bgpg, unsigned int time, unsigned int dumpDuration){
        cache->makeGraph(bgpg, time, dumpDuration);
        GraphToSave *gp =new GraphToSave(dumpath+"/graphdumps"+to_string(time)+"."+to_string(time+dumpDuration)+".graphml",bgpg->copy());
        graphsToSave.add(gp);
    }
    


    void run(){
        BGPMessage *bgpmessage;
        BGPGraph BGPg;
        BGPGraph *bgpg=&BGPg;
        string str;
        bool cont= true;
        previoustime =0;
        perfFile.open(perfFileName, std::ios_base::app);
        
#ifdef __linux
        prctl(PR_SET_NAME,"BGPSAVER");
#endif
        while (cont){
           if (infifo.try_take(bgpmessage, std::chrono::milliseconds(1200000))==BlockingCollectionStatus::TimedOut){
               cont = false;
               cout<< "Data Famine Saver"<<endl;
//               bgpg= new BGPGraph();
               saveGraph(bgpg, time, dumpDuration);
               stats.g=bgpg;
               stats.makeReport(lastStats, previoustime);
               perfFile<<stats.toJson(str)<<","<<endl;
               perfFile<<stats.toJson(str)<<","<<endl;
               cout<<"save !!!!!!!!!!!!!!!!!!!!" + to_string(time) + " to " + to_string(time + dumpDuration)<<endl;
               break;
            } else {
                stats.update(bgpmessage);
            }
            if (bgpmessage->timestamp>time+dumpDuration-1){
                BGPEvent *event;
                event= new BGPEvent(time+dumpDuration-1,CAPTTIME);
                event->hash=0;
                cache->bgpRedis->add(event);
                previoustime=bgpmessage->timestamp;
                saveGraph(bgpg, time, dumpDuration);
                table->windowtime=time+dumpDuration;
                table->duration=dumpDuration;
                stats.g=bgpg;
                stats.makeReport(lastStats, previoustime);
                perfFile<<stats.toJson(str)<<","<<endl;
                lastStats.fill(stats);
                cout<<"save !!!!!!!!!!!!!!!!!!!!" + to_string(time) + " to " + to_string(time + dumpDuration)<<endl;
                time=((int)bgpmessage->timestamp/dumpDuration)*dumpDuration;
            }
            count++;
//            if (count%10000 ==0){
//                cout<<"Stats:";
//                stats.printStr();
//            }
        }
//        perfFile<<"}"<<endl;
        perfFile.close();
    }

private:
    BlockingCollection<GraphToSave *> &graphsToSave;
    BGPGraph *bgpg = NULL;
    int time, dumpDuration;
    BlockingCollection<BGPMessage *> &infifo;
    Stats stats;
    Stats lastStats;
    unsigned int previoustime;
    RIBTable *table;
    int count =0;
    string perfFileName;
    std::ofstream perfFile;
    std::ofstream eventFile;
    string dumpath;
};
#endif //BGPGEOPOLITICS_BGPSAVER_H
