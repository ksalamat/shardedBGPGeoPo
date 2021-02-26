//
// Created by Kave Salamatian on 24/11/2018.
//

#ifndef BGPGEOPOLITICS_BGPGEOPOLITICS_H
#define BGPGEOPOLITICS_BGPGEOPOLITICS_H
#include "stdint.h"
extern "C" {
#include "bgpstream.h"
}

//#include "cache.h"
#include <string>
#include "cache_structures.h"


using namespace std;



class BGPCache;
class PrefixPath;
typedef std::shared_ptr<PrefixPath> SPrefixPath;
class Path;
class RIBElement;
class CollectorElement;
class Peer;
class AS;
typedef std::shared_ptr<AS> SAS;


enum Category{None=0, AADiff=1,AADup=2, WADup=3, WWDup=4, Flap=5, Withdrawn=6, STOP=7, UNDFND=9};



class BGPMessage{
public:
    int poolOrder;
    long messageOrder;
    string collector;
    bgpstream_elem_type_t  type;
    uint32_t  timestamp;
    bgpstream_ip_addr_t  peerAddress;
    Peer*  peer;
    SAS dest;
    bgpstream_ip_addr_t   nextHop;
    bgpstream_pfx_t pfx;
    string pfxStr;
    RIBElement* trieElement=NULL;
    vector<unsigned int> asPath;
    vector<unsigned int> shortPath;
    SPrefixPath prefixPath=NULL;
    unsigned int pathHash;
    string encodedPath;
    bool newPath = false;
    Category category = UNDFND;
    string pathStr;

    BGPMessage(int order);
    bool fill(long order, bgpstream_elem_t *elem, unsigned int time, string collector);
    double fusionRisks(double geoRisk, double secuRisk, double otherRisk);
    bool shortenPath();
    string pathString();
    string pfxString();
    unsigned int getIP();
    bool setPath(unsigned int time);
    pair<bool,unsigned int> checkRedis(SPrefixPath path, unsigned int timestamp);
};

class BGPMessageComparer {
public:
    BGPMessageComparer() {}

    int operator() (const BGPMessage* bgpMessage, const BGPMessage* new_bgpMessage) {
        if (bgpMessage->messageOrder < new_bgpMessage->messageOrder)
            return 1;
        else if (bgpMessage->messageOrder > new_bgpMessage->messageOrder)
            return -1;
        else
            return 0;
    }
};


string to_myencoding(unsigned int val);
string to_myencodingPath(unsigned int *path, int length);
string to_myencodingPref(bgpstream_pfx_t *inpfx);
unsigned int from_myencoding(string str);
void from_myencodingPath(string str, vector<unsigned int> &vect);
void from_myencodingPref(string str, bgpstream_pfx_t *inpfx );
#endif //BGPGEOPOLITICS_BGPGEOPOLITICS_H

