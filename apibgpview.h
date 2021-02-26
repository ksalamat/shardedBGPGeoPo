//
// Created by Kave Salamatian on 2018-12-26.
//

#ifndef BGPGEOPOLITICS_APIBGPVIEW_H
#define BGPGEOPOLITICS_APIBGPVIEW_H
#include "BGPGeopolitics.h"
#include "cache.h"
#include <curl/curl.h> 
#include "json.hpp"
#include <sqlite3.h>
class BGPCache;
class AS;
typedef std::shared_ptr<AS> SAS;

class BGPAPI{
public:
    SAS as;
    unsigned int timestamp;
    BGPAPI(SAS as,unsigned int timestamp):as(as),timestamp(timestamp){};
};
typedef BGPAPI* SBGPAPI;

class APIbgpview{
    public :
    APIbgpview(BlockingCollection<SBGPAPI> &infifo):infifo(infifo){}
    void insert(SAS as);
    void update(SAS as);
    void run();
private:
    BlockingCollection<SBGPAPI> &infifo;
};

#endif //BGPGEOPOLITICS_APIBGPVIEW_H
