//
// Created by Kave Salamatian on 2018-12-27.
//
#include "BGPGeopolitics.h"
#include "cache.h"
#include <curl/curl.h>
#include "json.hpp"
#include <sqlite3.h>
#include <algorithm>
#include "BGPEvent.h"



extern BGPCache *cache;
extern sqlite3 *db;

using namespace std;


void APIbgpview::insert(SAS as){
    if ((as->getStatus() & BOGUS) ==0)
        as->insertDB();
    
}

void APIbgpview::update(SAS as){
    as->updateBGPView();
    if ((as->getStatus() & BOGUS) ==0)
        as->updateDB();
}



void APIbgpview::run(){
    SBGPAPI data;
    SAS as;
    unsigned int timestamp;
    while(true){
        infifo.take(data);
        as= data->as;
        timestamp= data->timestamp;
        if (!as){
            break;
        } else {
            if (as->getName()=="XX") {
                insert(as);
            } else{
                update(as);
            }
            BGPEvent *event = new BGPEvent(timestamp ,NEWAS);
            event->hash=as->getNum();
            as->toMap(event->map);
            cache->bgpRedis->add(event);
            as->setObserved();
        }
    }
}
