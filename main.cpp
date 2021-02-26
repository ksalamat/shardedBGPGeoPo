#include <iostream>
#include <thread>
#include <list>
#include "BlockingQueue.h"
#include "BGPGeopolitics.h"
#include "BGPSource.h"
#include "cache.h"
#include "BGPGraph.h"
#include "BGPTables.h"
#include "BGPSaver.h"
#include "BGPRedis.hpp"

BGPCache *cache;
sqlite3 *db;
RIBTable *bgpTable;



class Wrapper {
    std::thread source, save, redis;
public:
    Wrapper(unsigned int t_start, unsigned int t_end, unsigned int dumpDuration, std::map<std::string, unsigned short int>& collectors ,  std::string& captype, int version, string path, string ppath,int port, int dbase) {
        Redis *_redis;
        unsigned int t_begin=t_start;
        PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> toTableFlag(10000);
        BlockingCollection<BGPMessage *> toSaver(10000000);
        BGPGraph g;
        BGPMessagePool bgpMessagePool(10000);
        bgpTable = new RIBTable(t_start, dumpDuration);
        int numShards=8;
        ShardedBGPRedis *bgpRedis= new ShardedBGPRedis("127.0.0.1", port, dbase,numShards);
        BGPCache bgpCache(path+"resources/as.sqlite",&g, bgpRedis, collectors, t_start,ppath);
        cache= &bgpCache;
        int numofWorkers=8;
        vector<std::thread> workers(numofWorkers);
        vector<std::thread> bgpSavers(4);
        bool dataInRedis=false;
        bgpRedis->setSavingMode();
        vector<string> strs;
        _redis=bgpRedis->getRedis(0);
        _redis->lrange("CAPT", 0,0,std::inserter(strs, strs.begin()));
        if (strs.size()>0){
            dataInRedis=true;
            t_begin=from_myencoding(strs[0])+1;
            bgpRedis->populate();
        } else {
            bgpCache.fillASCache();
        }

        if (dataInRedis &&(t_begin>=t_start-8*60*60)){ //it t_begin within 8 hours of t_start
            captype="U";
        } else
            captype="R";
        BlockingCollection<GraphToSave *> graphsToSave(4);
        ScheduleSaver *saver = new ScheduleSaver(t_begin, dumpDuration, toSaver, bgpTable, graphsToSave, ppath);
        BGPSaver *bgpSaver= new BGPSaver(graphsToSave);
        for(int i=0;i<3;i++){
            bgpSavers[i]=std::thread(&BGPSaver::run, bgpSaver);
        }
        BGPSource *bgpsource = new BGPSource(&bgpMessagePool, toTableFlag,  t_begin, t_end, dumpDuration, collectors ,  captype, 4);
        TableFlagger *tableFlagger = new TableFlagger(toTableFlag, toSaver, bgpTable, bgpsource, 4);
        source = std::thread(&BGPSource::run, bgpsource);
        for (int i=0;i<numofWorkers;i++){
            workers[i]=std::thread(&TableFlagger::run, tableFlagger);
        }
        save = std::thread(&ScheduleSaver::run, saver);
        bgpRedis->run();
        for (int i=0;i<numofWorkers;i++){
            workers[i].join();
        }
        source.join();
        cache->apiThread.join();
        save.join();
    }
    
};

int main(int argc, char **argv) {
    unsigned int start =1573689600;
    unsigned int end = start+16*24*60*60;

    std::string mode ="BR";
    unsigned int dumpDuration =600;
    string path,ppath;
    int dbase, port;
    if( argc > 2 ) {
        string command1(argv[1]);
        if (command1 == "-T") {
            start=stoul(argv[2]);
            end=stoul(argv[3]);
        }
        string command2(argv[4]);
        if (command2 =="-D")
            dumpDuration= stoi(argv[5]);
        string command3(argv[6]);
        if (command3 =="-P")
            path=argv[7];
        string command4(argv[8]);
        if (command4 =="-PP")
            ppath=argv[9];
        string command5(argv[10]);
        if (command5=="-PORT")
           port=stoi(argv[11]);
        string command6(argv[12]);
        if (command6=="-DB")
           dbase=stoi(argv[13]);
    }
    std::map<std::string, unsigned short int > collectors;
    collectors.insert(pair<string, unsigned short int >("rrc00",0));
    collectors.insert(pair<string, unsigned short int >("rrc01",1));
    collectors.insert(pair<string, unsigned short int >("rrc02",2));
    collectors.insert(pair<string, unsigned short int >("rrc03",3));
    collectors.insert(pair<string, unsigned short int >("rrc04",4));
    collectors.insert(pair<string, unsigned short int >("rrc05",5));
    collectors.insert(pair<string, unsigned short int >("rrc06",6));
    collectors.insert(pair<string, unsigned short int >("rrc09",7));
    collectors.insert(pair<string, unsigned short int >("rrc10",8));
    collectors.insert(pair<string, unsigned short int >("rrc11",9));
    collectors.insert(pair<string, unsigned short int >("rrc12",10));
    collectors.insert(pair<string, unsigned short int >("rrc13",11));
    collectors.insert(pair<string, unsigned short int >("rrc14",12));
    collectors.insert(pair<string, unsigned short int >("rrc15",13));
    collectors.insert(pair<string, unsigned short int >("rrc16",14));
    collectors.insert(pair<string, unsigned short int >("rrc17",15));
    collectors.insert(pair<string, unsigned short int >("rrc18",16));
    collectors.insert(pair<string, unsigned short int >("rrc19",17));
    collectors.insert(pair<string, unsigned short int >("rrc20",18));
    collectors.insert(pair<string, unsigned short int >("rrc21",19));
    Wrapper *w = new Wrapper(start, end, dumpDuration, collectors, mode,4, path, ppath, port, dbase);
    return 0;
}


