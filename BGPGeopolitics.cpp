//
// Created by Kave Salamatian on 2018-12-01.
//
#include "BGPGeopolitics.h"
#include "cache.h"
#include "BGPEvent.h"
#include "BGPRedis.hpp"
#define MAX_AS_NUMBER 1000000

extern BGPCache *cache;

BGPMessage::BGPMessage(int order): poolOrder(order){}

bool BGPMessage::fill(long order, bgpstream_elem_t *elem, unsigned int time, std::string incollector){
    bgpstream_as_path_iter_t iter;
    bgpstream_as_path_seg_t *seg;
    unsigned int asn;
    std::map<std::string, Path *>::iterator it1;
    string pathHashStr;

    messageOrder = order;
    category = None;
    prefixPath = NULL;
    pathHash=0;
    newPath = false;
    asPath.clear();
    shortPath.clear();
    type = elem->type;
    
    
//    memcpy(&peerAddress, &elem->peer_ip , sizeof(bgpstream_addr_storage_t));
    memcpy(&nextHop, &elem->nexthop, sizeof(bgpstream_ip_addr_t));
    memcpy(&pfx, &elem->prefix,sizeof(bgpstream_pfx_t));
    timestamp = time;
    collector = incollector;
    if (type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT || type == BGPSTREAM_ELEM_TYPE_RIB){
        bgpstream_as_path_iter_reset(&iter);
        while ((seg = bgpstream_as_path_get_next_seg(elem->as_path, &iter)) != NULL) {
            switch (seg->type) {
                case BGPSTREAM_AS_PATH_SEG_ASN:
                    asn = ((bgpstream_as_path_seg_asn_t *) seg)->asn;
                    asPath.push_back(asn);
                    break;
                case BGPSTREAM_AS_PATH_SEG_SET:
                    /* {A,B,C} */
                    break;
                case BGPSTREAM_AS_PATH_SEG_CONFED_SET:
                    /* [A,B,C] */
                    break;
                case BGPSTREAM_AS_PATH_SEG_CONFED_SEQ:
                    /* (A B C) */
                    break;
            }
        }
        if (shortenPath()) {
            if (shortPath.size()==0) {
                return false;
            } else {
                peer = new Peer(elem->peer_asn);
                auto res=cache->peersMap.insert(make_pair(asPath[0],peer));
                if (!res.first){
                    delete peer;
                    peer=res.second;
                }
                pathStr.clear();
                pathStr=pathString();
                pfxStr.clear();
                pfxStr=pfxString();
                return true;
            }
        } else{
            return false;
        }
    } else if (type == BGPSTREAM_ELEM_TYPE_WITHDRAWAL){
        peer = new Peer(elem->peer_asn);
        auto res=cache->peersMap.insert(make_pair(elem->peer_asn,peer));
        if (!res.first){
            delete peer;
            peer =res.second;
        }
        return true;
    }
    return false;
}

bool BGPMessage::setPath(unsigned int time){
    auto p=cache->pathsMap.find(pathStr);
    if (p.first){
        prefixPath = p.second;
        pathHash =prefixPath->hash;
        dest = cache->asCache[shortPath.back()];
        return true;
    } else {
        SPrefixPath path=std::make_shared<PrefixPath>(this, timestamp);
        if (!checkRedis(path,timestamp).first){
            // this is a new path
            path->lastChange = time;
            path->prefNum=1;
            path->active = true;
            path->setPathActive(time);
            path->meanDown = 0;
            path->lastActive = time;
            path->announcementNum=1;
            path->prefNum=1;
            dest = cache->asCache[shortPath.back()];
            pathHash =path->hash;
            return true;
        } else {
            cache->pathsMap.strCacheMissed();
        }
    }
    dest = cache->asCache[shortPath.back()];
    return false;
}

bool BGPMessage::shortenPath() {
    unsigned int prev = 0;
    for (auto asn:asPath) {
        if (asn > MAX_AS_NUMBER)
            return false;
        if (asn != prev) {
            shortPath.push_back(asn);
            prev = asn;
        }
    }
    shortPath.resize(shortPath.size());
    return true;
}

string BGPMessage::pathString(){
    string pathString;
    long shortPathLength=shortPath.size();
    for (int i=0;i<shortPathLength-1;i++) {
        pathString += to_string(shortPath[i])+",";
    }
    pathString += to_string(shortPath[shortPathLength-1]);
    return pathString;
}

string BGPMessage::pfxString(){
    string pfxStr;
    char buffer[20];
    bgpstream_pfx_snprintf(buffer, 20, &pfx);
    pfxStr=string(buffer);
    return pfxStr;
}

pair<bool,unsigned int> BGPMessage::checkRedis(SPrefixPath path, unsigned int timestamp){
    encodedPath=to_myencodingPath(shortPath.data(),shortPath.size());
    if (cache->pathsBF.contains(path->str())){
        unsigned int hash1=path->getPeer();
        Redis *_redis1=cache->bgpRedis->getRedis(hash1);
        auto hashStr=_redis1->hget("PATH2ID",encodedPath);
        if (hashStr){
            Redis *_redis2=cache->bgpRedis->getRedis(hash1);
            auto str=_redis2->hget("PATHS",*hashStr);
            if (str) {
                path->fromRedis(*str);
                auto ret=cache->pathsMap.insert(path->hash,path,timestamp);
                if(ret.first){
                    prefixPath = path;
                    pathHash= path->hash;
                    return make_pair(true, path->hash);
                } else {
                    prefixPath=ret.second;
                    pathHash=prefixPath->hash;
                    return make_pair(true, pathHash);
                }
            }
        }
    }
    //the path is not in Redis
    auto ret=cache->pathsMap.insert(path,timestamp);
    prefixPath=ret.second;
    pathHash=prefixPath->hash;
    if (pathHash==0){
        int bug=1;
    }
    return make_pair(false, ret.second->hash);
}

string to_myencoding(unsigned int val){
    string str="";
    int l;
    if (val == 0){
        return str+ (char)(33);
    } else {
        while(val !=0){
            l = val%92;
            val=(val-l)/92;
            if (l<25)
                str +=(char)(33+l);
            else
                str +=(char)(33+l+1);
        }
        return str;
    }
}

string to_myencodingPath(unsigned int* path, int length){
    string str="", str1;
    for(int i=0; i<length; i++){
        str1=to_myencoding(path[i]);
        switch (str1.length()) {
            case 1:
                str1=str1+"!!!";
                break;
            case 2:
                str1=str1+"!!";
                break;
            case 3:
                str1=str1+"!";
            default:
                break;
        }
        str +=str1;
    }
    return str;
}

string to_myencodingPref(bgpstream_pfx_t *inpfx){
    string str;
    unsigned long val=inpfx->mask_len, l;
    unsigned int address;
    memcpy(&address, &inpfx->address.addr, sizeof(4));
    val=(val<<32)+address;
    
    if (val == 0){
        return str+ (char)(33);
    } else {
        while(val !=0){
            l = val%92;
            val=(val-l)/92;
            if (l<25)
                str +=(char)(33+l);
            else
                str +=(char)(33+l+1);
        }
        return str;
    }
}

unsigned int from_myencoding(string str){
    unsigned int val=0,coef=1,l;
    for(unsigned long i=0;i<str.length();i++){
        if (str[i]<58)
            l=str[i]-33;
        else
            l=str[i]-33-1;
        val = val+(unsigned char)l*coef;
        coef = 92*coef;
    }
    return val;
}

void from_myencodingPath(string str, vector<unsigned int> &vect){
    string str1;
    for(int i=0;i<str.length();i=i+4){
        str1=str.substr(i,4);
        vect.push_back(from_myencoding(str1));
    }
}

void from_myencodingPref(string str, bgpstream_pfx_t *inpfx ){
    unsigned long val=0,coef=1,l, ipval;
    unsigned int len;
    string pfxstr="";
    for(unsigned long i=0;i<str.length();i++){
        if (str[i]<58)
            l=str[i]-33;
        else
            l=str[i]-33-1;
        val += (unsigned char)l*coef;
        coef = 92*coef;
    }
    len= (uint8_t)((val & 0X000000FF00000000)>>32);
    ipval = val & 0x00000000FFFFFFFF;
    for (int i=0;i<4;i++){
        pfxstr += to_string(ipval % 256);
        if (i<3)
            pfxstr +=".";
        ipval = ipval/256;
    }
    pfxstr+="/"+to_string(len);
    bgpstream_str2pfx(pfxstr.c_str(),inpfx);
 }
