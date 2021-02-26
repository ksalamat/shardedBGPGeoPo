//
//  BGPSource.cpp
//  BGPGeo
//
//  Created by Kave Salamatian on 18/01/2019.
//  Copyright © 2019 Kave Salamatian. All rights reserved.
//

#include <stdio.h>
#include "BGPSource.h"
#include "BGPTables.h"
#include <chrono>
#ifdef __linux
    #include <sys/prctl.h>
#endif

extern BGPCache *cache;

BGPMessagePool::BGPMessagePool(int capacity): capacity(capacity), bgpMessages(capacity) {
    for(int i=0;i<capacity;i++){
        bgpMessages.add(new BGPMessage(i));
    }
}

BGPMessage* BGPMessagePool::getBGPMessage(long order, bgpstream_elem_t *elem, unsigned int time, std::string collector){
    BGPMessage *bgpMessage;
    bgpMessages.take(bgpMessage);
    if (bgpMessage->fill(order, elem, time, collector)){
        return bgpMessage;
    } else {
        returnBGPMessage(bgpMessage);
        return NULL;
    }
}

void BGPMessagePool::returnBGPMessage(BGPMessage* bgpMessage) {
    bgpMessage->asPath.clear();
    bgpMessage->shortPath.clear();
    bgpMessage->prefixPath=NULL;
    bgpMessage->peer=NULL;
    bgpMessage->dest=NULL;
    bgpMessages.add(bgpMessage);
}

bool BGPMessagePool::isPoolAvailable(){
    return !bgpMessages.is_empty();
}

class Record{
public:
    unsigned int time;
    string collector;
    Record(unsigned int time, string collector): time(time), collector(collector){};
};


BGPSource::BGPSource(BGPMessagePool *bgpMessagePool,PriorityBlockingCollection<BGPMessage *,
                     PriorityContainer<BGPMessage *, BGPMessageComparer>> &fifo,
                     unsigned int t_start, unsigned int t_end, unsigned int dumpDuration, map<std::string, unsigned short int> &collectors, std::string &captype,
                     int version) :bgpMessagePool(bgpMessagePool), fifoQueue(fifo), t_start(t_start), t_end(t_end), dumpDuration(dumpDuration), version(version), collectors(collectors){
    if (captype=="R")
        mode =0;
    else
        mode=1;
    /* Set metadata filters */
}

int BGPSource::run() {
    bgpstream_elem_t *elem;
    BGPMessage *bgpMessage;
    unsigned long order=0;
    std::string collector;
    unsigned int t_begin=t_start, time;
    bool proceed = false;
    map<std::string, unsigned int > records;
    bgpstream_t *bs;
    bgpstream_record_t *record;
    bool first = true;
    std::chrono::high_resolution_clock::time_point start=std::chrono::high_resolution_clock::now(),end;
    std::chrono::duration<double, std::milli> processDuration;
#ifdef __linux
    prctl(PR_SET_NAME,"BGPSOURCE");
#endif
    start = std::chrono::high_resolution_clock::now();
    if (mode ==0) {
	cout<<"Begin to get a full dump"<<endl;
        bs = bgpstream_create();
        for (auto const &collector : collectors) {
            bgpstream_add_filter(bs, BGPSTREAM_FILTER_TYPE_COLLECTOR, collector.first.c_str());
        }
        bgpstream_add_filter(bs, BGPSTREAM_FILTER_TYPE_RECORD_TYPE, "ribs");
        bgpstream_add_interval_filter(bs, t_start-8*60*60+1, t_start);
        bgpstream_start(bs);
        while (bgpstream_get_next_record(bs, &record) > 0) {
            /* Ignore invalid records */
            if (record->status != BGPSTREAM_RECORD_STATUS_VALID_RECORD) {
                continue;
            }
//            std::string collector(record->attributes.dump_collector);
            collector = string{record->collector_name};
            if (first){
                t_begin=record->time_sec;
                first= false;
            }
            while (bgpstream_record_get_next_elem(record, &elem)>0) {
                proceed = false;
                if (elem->type == BGPSTREAM_ELEM_TYPE_RIB) {
                    if (elem->prefix.address.version == BGPSTREAM_ADDR_VERSION_IPV6) {
                        if ((version==6) || (version==64)){
                            proceed=true;
                        }
                    } else {
                        if ((version==4) || (version==64)){
                            proceed=true;
                        }
                    }
                }
                if (proceed) {
                    count++;
                    if (count % 1000000 == 0) {
                        end = std::chrono::high_resolution_clock::now();
                        processDuration=(end-start);
                        int processTime= processDuration.count();
                        std::cout<<count<<","<<fifoQueue.size()<<","<<processTime<<" msec"<<std::endl;
                        start=end;
                    }
                    bgpMessage = bgpMessagePool->getBGPMessage(order++, elem, record->time_sec, collector);
                    if (bgpMessage != NULL){
                        fifoQueue.add(bgpMessage);
                    }
                }
            }
        }
        BGPEvent *event;
        event= new BGPEvent(t_begin,CAPTBEGIN);
        event->hash=0;
        cache->bgpRedis->add(event);
        cout<<"End full dump"<<endl;
        bgpstream_destroy(bs);
    }
    unsigned int interval = max((int)(4*60*60/dumpDuration), 1);
    unsigned int t_final=t_begin+(ceil((t_end-t_begin)*1.0/dumpDuration))*dumpDuration+1;
    for (unsigned t1=t_begin;t1<t_end;t1 +=interval*dumpDuration){
        bs = bgpstream_create();
        for (auto const &collector : collectors) {
            bgpstream_add_filter(bs, BGPSTREAM_FILTER_TYPE_COLLECTOR, collector.first.c_str());
        }
        bgpstream_add_filter(bs, BGPSTREAM_FILTER_TYPE_RECORD_TYPE, "updates");
        bgpstream_add_interval_filter(bs, t1, min(t_end, t1+interval*dumpDuration-1));
        bgpstream_start(bs);
        /* Read the stream of records */
        while (bgpstream_get_next_record(bs, &record) > 0) {
            /* Ignore invalid records */
            if (record->status != BGPSTREAM_RECORD_STATUS_VALID_RECORD) {
                continue;
            }
            //        std::string collector(record->attributes.dump_collector);
            collector = string{record->collector_name};
            time = record->time_sec;
            /* Extract elems from the current record */
            //        while ((elem = bgpstream_record_get_next_elem(record)) != NULL) {
            while (bgpstream_record_get_next_elem(record, &elem)>0) {
                proceed = false;
                if (elem->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT ||
                    elem->type == BGPSTREAM_ELEM_TYPE_WITHDRAWAL) {
                    if (elem->prefix.address.version == BGPSTREAM_ADDR_VERSION_IPV6) {
                        if ((version==6) || (version==64)){
                            proceed=true;
                        }
                    } else {
                        if ((version==4) || (version==64)){
                            proceed=true;
                        }
                    }
                }
                if (proceed) {
                    count++;
                    if (count % 1000000 == 0) {
                        end = std::chrono::high_resolution_clock::now();
                        processDuration=(end-start);
                        int processTime= processDuration.count();
                        std::cout<<count<<","<<fifoQueue.size()<<","<<processTime<<" msec"<<std::endl;
                        start=end;
                    }
                    bgpMessage = bgpMessagePool->getBGPMessage(order++, elem, time, collector);
                    if (bgpMessage != NULL){
                        fifoQueue.add(bgpMessage);
                    }
                }
            }
        }
        bgpstream_destroy(bs);
    }
    bgpMessage = new BGPMessage(order);
    bgpMessage->category = STOP;
    fifoQueue.add(bgpMessage);
    std::cout << "FINISH" << std::endl;
//    bgpstream_destroy(bs);
    return 0;
}

