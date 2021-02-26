//
// Created by Kave Salamatian on 2018-12-01.
//

#include <sqlite3.h>
#include <set>
#include <map>
#include <unordered_map>
#include <boost/range/combine.hpp>
#include "BGPGraph.h"
#include "BGPTables.h"
#include "cache.h"
#include "BGPGeopolitics.h"
#include "bgpstream_utils_patricia.h"
#include "json.hpp"
#include <boost/algorithm/string.hpp>



//using boost::get;
using namespace std;
using namespace tbb;
using json = nlohmann::json;


extern BGPCache *cache;

float coeff = 0.9;

string contents;
size_t handle_data(void *ptr, size_t size, size_t nmemb, void *stream)
{
    int numbytes = size*nmemb;
    // The data is not null-terminated, so get the last character, and replace
    // it with '\0'.
    char lastchar = *((char *) ptr + numbytes - 1);
    *((char *) ptr + numbytes - 1) = '\0';
    contents.append((char *)ptr);
    contents.append(1,lastchar);
    *((char *) ptr + numbytes - 1) = lastchar;  // Might not be necessary.
    return size*nmemb;
}


int BGPCache::fillASCache() {
    sqlite3_stmt *stmt;
    unsigned int asn;
    string name;
    string country;
    string RIR;
    float risk;
    int count = 0;
    string sql;

    cout << "Starting fill AS Cache" << endl;
    /* Create SQL statement */
    sql = "SELECT asNumber, name, country, RIR, riskIndex, geoRisk, perfRisk, secuRisk, otherRisk, observed FROM asn";
    /* Execute SQL statement */
    sqlite3_prepare(db, sql.c_str(), sql.length(), &stmt, NULL);
    bool done = false;
    while (!done) {
        switch (sqlite3_step(stmt)) {
            case SQLITE_ROW: {
                asn = atoi((const char *) sqlite3_column_text(stmt, 0));
                name = string((const char *) sqlite3_column_text(stmt, 1));
                if (name =="")
                    name = "??",
                country = string((const char *) sqlite3_column_text(stmt, 2));
                if (country =="")
                    country = "??",
                RIR = string((const char *) sqlite3_column_text(stmt, 3));
                if (RIR =="")
                    RIR = "??",

                risk = atof((const char *) sqlite3_column_text(stmt, 4));
                SAS as = std::make_shared<AS>(asn, name, country, RIR, risk);
                asCache.insert(pair<int, SAS>(asn, as));
                count++;
                break;
            }
            case SQLITE_DONE: {
                done = true;
                break;
            }
        }
    }
    sqlite3_reset(stmt);
    sqlite3_finalize(stmt);
    cout << count << " row processed" << endl;
    return 0;
}

void BGPCache::updateAS(SAS as, unsigned int time) {
    BGPAPI *data=new BGPAPI(as, time);
    toAPIbgpbiew.add(data);
}

SAS BGPCache::chkAS(unsigned int asn, unsigned int time){
    SAS as;

    map<unsigned int, SAS>::iterator it;
    //checks if the AS exists and if not update it
    auto p=asCache.find(asn);
    if (p.first){
        as=p.second;
        if (!as->isObserved()){
            if (time>0) {
                BGPEvent *event = new BGPEvent(time,NEWAS );
                as->toMap(event->map);
                event->hash= as->getNum();
                bgpRedis->add(event);
                as->setObserved();
            }
        }
        return as;
    } else {
        SAS as=std::make_shared<AS>(asn, "XX", "XX", "XX", 0.0);
        auto p=asCache.insert(make_pair(asn, as));
        if (p.first){
            updateAS(as,time);
            as->setObserved();
            as->touch();
            return as;
        } else {
            return p.second;
        }
    }
    return NULL;
}


long BGPCache::size_of(){
    long size1=0,size2=0,size3=0, size4=0, size5=0;
//    size1=prefixPeersMap.size()*(4+8);//
//    size2=pathsMap.size()*(4+8);
//    size3=asCache.size()*(4+8);
//    size4=pathsMap.size_of();
//    size5=prefixPeersMap.size_of();
    return size1+size2+size3+size4+size5;
}

void BGPCache::makeGraph(BGPGraph* g, unsigned int time, unsigned int dumpDuration){
    Link *link;
    unsigned long linkID;
    SPrefixPath path;
    string dumpath=ppath;
    BGPEvent *event;
    AS *as;
    std::chrono::high_resolution_clock::time_point start=std::chrono::high_resolution_clock::now(),end;
    bgpg=g;
//    g->clear();
    while (touchedASes.try_pop(as)){
        if (as->hasLinks()){
            if (as->isObserved()){
                as->checkVertex(bgpg);
            }
        } else {
            as->removeVertex(bgpg);
        }
        as->untouch();
        event = new BGPEvent(time, ASUPD);
        as->toMap(event->map);
        event->hash= as->getNum();
        cache->bgpRedis->add(event);
    }
    while (touchedLinks.try_pop(link)){
        linkID=link->linkID();
        if (link->isActive()) {
            link->checkEdge(bgpg);
        } else {
            link->removeEdge(bgpg);
        }
        link->unTouch();
        event = new BGPEvent(time, LNKUPD);
        link->toRedis(event->map);
        event->hash= std::hash<std::string>{}(link->str());
        cache->bgpRedis->add(event);
    }
}

PrefixPath::PrefixPath(){

}

PrefixPath::PrefixPath(BGPMessage *bgpMessage, unsigned int time){
    int i =0;
    
    collector =cache->collectors.find(bgpMessage->collector)->second;
    shortPathLength = bgpMessage->shortPath.size();
    if (bgpMessage->type != BGPSTREAM_ELEM_TYPE_WITHDRAWAL){
        shortPath = new unsigned int[shortPathLength];
    }
    i=0;
    for (auto as1:bgpMessage->shortPath){
        shortPath[i++]=as1;
        cache->chkAS(as1,time);
    }
    pathLength=bgpMessage->asPath.size();
    lastActive = time;
    lastChange = 0;
    active = false;
}

PrefixPath::PrefixPath(vector<unsigned int> &pathVect){
    shortPathLength = pathVect.size();
    shortPath = new unsigned int(shortPathLength);
    memcpy(shortPath,&pathVect[0],sizeof(int)*shortPathLength);
}



PrefixPath::PrefixPath(string str){
    fromRedis(str);
    for(int i=0;i<shortPathLength;i++){
        cache->chkAS(shortPath[i],lastActive);
    }
}


void PrefixPath::update(string str){
    fromRedis(str);
}

PrefixPath::~PrefixPath(){
    if (shortPath)
        delete shortPath;
}


int PrefixPath::size_of(){
//    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    int size=9*4+4+2;;
    size +=shortPathLength*4;
    return size;
}

double PrefixPath::getScore() const {
//    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    return shortPathLength*1.0;
}


string PrefixPath::sstr() const {
//    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    string pathString;
    for (int i=0;i<shortPathLength-1;i++) {
        pathString += to_string(shortPath[i])+",";
    }
    pathString += to_string(shortPath[shortPathLength-1]);
    return pathString;
}

string PrefixPath::str() const{
    return to_myencodingPath(shortPath, shortPathLength);
}


unsigned int PrefixPath::getPeer() const{
    return shortPath[0];
}

unsigned int PrefixPath::getDest() const{
//    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    return shortPath[shortPathLength-1];
}

bool PrefixPath::addPrefix(unsigned int time){
    sem.acquire();
    bool success=false;
    lastChange = time;
    if (prefNum==0){
        prefNum++;
        active = true;
        setPathActive(time);
        meanDown = coeff*meanDown+(1-coeff)*(time-lastChange);
        lastActive = time;
        success = true;
    } else {
        prefNum++;
    }
    sem.release();
    return success;
}

bool PrefixPath::erasePrefix(unsigned int time){
    sem.acquire();
    prefNum--;
    lastChange = time;
    if (prefNum==0){
        active = false;
        setPathNonActive(time);
        meanUp = coeff*meanUp+(1-coeff)*(time-lastActive);
        sem.release();
        BGPEvent *event = new BGPEvent(time, PATHNACT);
        toRedis(event->map);
        event->hash=getPeer();
        cache->bgpRedis->add(event);
        return true;
    }
    sem.release();
    return false;
}

bool PrefixPath::checkPrefix(bgpstream_pfx_t *pfx){
//    boost::shared_lock<boost::shared_mutex> lock(mutex_);
//    return activePrefixTrie->check(pfx);
    return true;
}

bool PrefixPath::equal(SPrefixPath path) const{
//    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    if (path->shortPathLength == shortPathLength){
        for (int i=0; i<shortPathLength;i++){
            if (path->shortPath[i]!=shortPath[i]){
                return false;
            }
        }
    }
    return true;
}

void PrefixPath::save(BGPCache *cache){
 //   boost::unique_lock<boost::shared_mutex> lock(mutex_);
    
//    activePrefixTrie->savePrefixes(this);
//    saved = true;
}

void PrefixPath::saveToRedis(unsigned timestamp) {
    BGPEvent *event = new BGPEvent(timestamp, NEWPATH);
    toRedis(event->map);
    event->hash=getPeer();
    cache->bgpRedis->add(event);
}

void PrefixPath::setHash(unsigned int h) {
    hash=h;
}

void PrefixPath::setPathActive(unsigned int time){
    Link *link;
    unsigned int tmp, src, dst;
    BGPEvent *event;
    unsigned long linkId;

    if (shortPathLength>1){
        for (int i=0; i<shortPathLength-1;i++){
            src = shortPath[i];
            dst = shortPath[i+1];
            if (src>dst){
                tmp = src;
                src = dst;
                dst = tmp;
            }
            linkId = src;
            linkId=(linkId<<32)+dst;
            auto ret1=cache->linksMap.find(linkId);
            if (!ret1.first) {
                link = new Link(src, dst, time);
                link->touch();
                auto ret2=cache->linksMap.insert(make_pair(linkId,link));
                if (ret2.first){
                    event = new BGPEvent(time, NEWLINK);
                    link->toRedis(event->map);
                    event->hash= std::hash<std::string>{}(link->str());
                    cache->bgpRedis->add(event);
                } else {
//                    delete link ; 
                    link=ret2.second;
                }
            } else {
                link=ret1.second;
            }
            link->addPath(time);
        }
    }
}

void PrefixPath::setPathNonActive( unsigned int time){
    unsigned int tmp, peer, asn, src,dst;
    unsigned long linkId;
    Link *link;
    asn = getDest();
    peer = getPeer();
    
    if (shortPathLength>1){
        for (int i=0; i<shortPathLength-1;i++){
            src = shortPath[i];
            dst = shortPath[i+1];
            if (src>dst){
                tmp = src;
                src = dst;
                dst = tmp;
            }
            linkId = src;
            linkId=(linkId<<32)+dst;
            auto ret=cache->linksMap.find(linkId);
            link = ret.second;
            if ((link) && (link->isActive())) {
                link->withdraw(time);
            }
        }
    }
}

void PrefixPath::toRedis(std::unordered_map<std::string, std::string> &map){
    string str="";
    str +=to_myencoding(hash)+":";
    //map["HSH"]=to_myencoding(hash);
    str +=to_myencodingPath(shortPath, shortPathLength)+":";
    str += to_myencoding(pathLength)+":";
    //map["PTL"]=to_myencoding(pathLength);
    str +=to_myencoding(prefNum)+":";
    //map["PFN"]=to_myencoding(prefNum);
    str +=to_myencoding(lastChange)+":";
    //map["LCH"]=to_myencoding(lastChange);
    str +=to_myencoding(lastActive)+":";
    //map["LAC"]=to_myencoding(lastActive);
    str +=to_myencoding(announcementNum)+":";
    //map["ANN"]=to_myencoding(announcementNum);
    str +=to_myencoding(AADiff)+":";
    //map["AAD"]=to_myencoding(AADiff);
    str +=to_myencoding(AADup)+":";
    //map["AAU"]=to_myencoding(AADup);
    str +=to_myencoding(WADup)+":";
    //map["WAD"]=to_myencoding(WADup);
    str +=to_myencoding(WWDup)+":";
    //map["WWD"]=to_myencoding(WWDup);
    str +=to_myencoding(Flap)+":";
    //map["FLP"]=to_myencoding(Flap);
    str +=to_myencoding(Withdraw)+":";
    //map["WTH"]=to_myencoding(Withdraw);
    str +=to_myencoding((int)meanUp*10000)+":";
    //map["MUP"]=to_myencoding((int)meanUp*10000);
    str +=to_myencoding((int)meanDown*10000)+":";
    //map["MDW"]=to_myencoding((int)meanDown*10000);
    str +=to_myencoding(collector)+":";
    //map["CLT"]=string(1,collector);
    if (active){
        str +="T:";
        //map["ACT"]="T";
    } else {
        str +="F:";
        //map["ACT"]="F";
    }
    //map["pathHash"]=to_myencodingPath(shortPath, shortPathLength);
    map["HSH"]=to_myencoding(hash);
    map["STR"]=str;
}

void PrefixPath::fromRedis(string str){
    vector<string> results;
    vector<unsigned int> pathVect;
    results.clear();
    boost::split(results,str, [](char c){return c == ':';});
    int i=0;
    hash = from_myencoding(results[i++]);
    from_myencodingPath(results[i++], pathVect);
    shortPathLength = pathVect.size();
    shortPath = new unsigned int[shortPathLength];
    memcpy(shortPath,&pathVect[0],sizeof(unsigned int)*shortPathLength);
    pathLength = from_myencoding(results[i++]);
    prefNum = from_myencoding(results[i++]);
    lastChange  = from_myencoding(results[i++]);
    lastActive = from_myencoding(results[i++]);
    announcementNum = from_myencoding(results[i++]);
    AADiff = from_myencoding(results[i++]);
    AADup = from_myencoding(results[i++]);
    WADup = from_myencoding(results[i++]);
    WWDup = from_myencoding(results[i++]);
    Flap = from_myencoding(results[i++]);
    Withdraw= from_myencoding(results[i++]);
    meanUp= from_myencoding(results[i++])/10000;
    meanDown = from_myencoding(results[i++])/10000;
    collector= from_myencoding(results[i++]);
    if (results[i++] == "T")
        active = true;
    else
        active = false;
}




AS::AS(int asn): asNum(asn){
    activePrefixTrie = new Trie();
    inactivePrefixTrie = new Trie();
}

AS::AS(int asn, string inname, string incountry, string inRIR, float risk): asNum(asn), name(inname), country(incountry), RIR(inRIR){
    activePrefixTrie = new Trie();
    inactivePrefixTrie = new Trie();
    observed = false;
}

AS::AS(std::unordered_map<string, string> asMap){
    asNum=from_myencoding(asMap["ASN"]);
    fromRedis(asMap["STR"]);
    activePrefixTrie = new Trie();
    inactivePrefixTrie = new Trie();
}
    
AS::~AS(){
    activePrefixTrie->clear();
    delete activePrefixTrie;
    inactivePrefixTrie->clear();
    delete inactivePrefixTrie;
}

void AS::updateInfo(std::unordered_map<string, string> asMap){
    activePrefixNum=from_myencoding(asMap["APN"]);
    allPrefixNum=from_myencoding(asMap["ALN"]);
    allPrefix24Num=from_myencoding(asMap["A24"]);
    activePathsNum=from_myencoding(asMap["ATN"]);
    cTime=from_myencoding(asMap["CTI"]);
    if (asMap["OBS"]=="T"){
        observed = true;
    } else {
        observed = false;
    }
    status=from_myencoding(asMap["STA"]);
    touch();
}

int AS::getStatus(){
    return status;
}

void AS::updateJSON(json &j){
    try{
        if ((j["status"] !="error") && j["data"]["description_short"]!="Unallocated"){
            if (!j["data"]["name"].is_null()) {
                name = j["data"]["name"];
            } else {
                name = "UNKNOWN";
            }
            if (!j["data"]["country_code"].is_null()) {
                country = j["data"]["country_code"];
            } else {
                country = "XX";
            }
            if (!j["data"]["rir_allocation"]["rir_name"].is_null()) {
                RIR = j["data"]["rir_allocation"]["rir_name"];
            } else {
                RIR = "UNKNOWN";
            }
            if (!j["data"]["owner_address"].is_null()){
                ownerAddress = "";
                for (json::iterator it = j["data"]["owner_address"].begin(); it != j["data"]["owner_address"].end(); ++it) {
                    ownerAddress += *it;
                    ownerAddress +=",";
                }
            } else {
                ownerAddress="UNKNOWN";
            }
            if (!j["data"]["date_updated"].is_null()){
                lastChanged = j["data"]["date_updated"];
            } else {
                lastChanged = "UNKNOWN";
            }
            if (!j["data"]["description_short"].is_null()){
                description = j["data"]["description_short"];
            } else {
                description = "UNKNOWN";
            }
            if (!j["data"]["rir_allocation"]["date_allocated"].is_null()){
                dateCreated = j["data"]["rir_allocation"]["date_allocated"];
            } else {
                dateCreated = "UNKNOWN";
            }
            
        } else {
            status |= BOGUS;
        }
    } catch (json::parse_error) {
        cout << "JSON Error :" << asNum << "," << j << endl;
    }
    touch();
}



void AS::updateBGPView() {
    CURL *curl;
    CURLcode res;
    string url="";
    string contents = "";
    curl = curl_easy_init();
    if (curl) {
        url = "http://api.bgpview.io/asn/" + to_string(asNum);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        /* example.com is redirected, so we tell libcurl to follow redirection */
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, handle_data);
        /* Perform the request, res will get the return code */
        res = curl_easy_perform(curl);
        /* Check for errors */
        if (res != CURLE_OK)
            fprintf(stderr, "curl_easy_perform() failed: %s\n",
                    curl_easy_strerror(res));
        /* always cleanup */
        curl_easy_cleanup(curl);
    }
    json j = json::parse(contents);
    updateJSON(j);
}

void AS::insertDB(){
    sqlite3_stmt *stmt=NULL;
    erase_all(name, "'");
    erase_all(ownerAddress, "'");
    erase_all(name, "'");
    erase_all(description,"'");
    ownerAddress.erase(std::remove(ownerAddress.begin(), ownerAddress.end(), '\''), ownerAddress.end());
    string sql = "INSERT INTO asn VALUES (" + to_string(asNum) + ",'" + name + "','" +
                 country + "','" + RIR + "'," + to_string(0.0) + "," + to_string(0.0) + "," +
                 to_string(0.0) + "," + to_string(0.0) + "," + to_string(0.0) + "," + to_string(0)+  ",'" +
                 ownerAddress + "','" + lastChanged +"','" + dateCreated+ "','" + to_string(1)+ "','" + description+"')";
    sqlite3_prepare_v2(db, sql.c_str(),-1, &stmt, NULL);//preparing the statement
    auto rc = sqlite3_step(stmt);//executing the statement
    if (rc != SQLITE_DONE) {
        cout<<"ERROR inserting data: "+ string(sqlite3_errmsg(db))<<endl;
    }
    sqlite3_finalize(stmt);
}

void AS::updateDB(){
    sqlite3_stmt *stmt;
    string sql = "UPDATE asn SET country =\"" + country + "\" ,name=\"" + name + "\" WHERE asNumber=" +
                 to_string(asNum);
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL);//preparing the statement
    sqlite3_step(stmt);//executing the statement
    sqlite3_finalize(stmt);
}

void AS::setObserved(){
    observed =true;
}

bool AS::isObserved(){
    return observed;
}

void AS::touch(){
    if (!touched){
        cache->touchedASes.push(this);
    }
    touched = true;
}

void AS::untouch(){
    touched = false;
}

bool AS::isTouched(){
    return touched;
}

bool AS::checkOutage(){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    activePrefixNum = activePrefixTrie->prefixNum();
    allPrefixNum =  activePrefixNum+ inactivePrefixTrie->prefixNum();
    
    if ((activePrefixNum<0.3*allPrefixNum) &&(allPrefixNum>10))  {
        status |= OUTAGE;
        return true;
    }   else {
        if (status & OUTAGE) {
            status &= ~OUTAGE;
        }
        return false;
    }
}

string AS::getName(){
    return name;
}

unsigned int AS::getNum(){
    return asNum;
}

int AS::size_of(){
    int size =4+4*8+4*4+1;
    size += name.length();
    size += country.length();
    size += RIR.length();
    //        for(auto it=activePrefixMap.begin();it!=activePrefixMap.end();it++)
    //size += activePrefixMap.size()*(4+8);vert
    return size;
}

boost::graph_traits<Graph>::vertex_descriptor AS::checkVertex(BGPGraph *bgpg){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    if (vertex==-1){
        vertex = bgpg->add_vertex(VertexP{to_string(asNum), country, name, cTime, (int)activePrefixTrie->prefixNum(), (int)activePrefixTrie->prefixNum(),(int)activePrefixTrie->prefix24Num(), (int)activePrefixTrie->prefix24Num(), (int)activePathsNum });
    } else {
        bgpg->set_vertex(vertex, VertexP{to_string(asNum), country, name, cTime, (int)activePrefixTrie->prefixNum(), (int)activePrefixTrie->prefixNum(),(int)activePrefixTrie->prefix24Num(), (int)activePrefixTrie->prefix24Num(), (int)activePathsNum });
    }
    return vertex;
}

void AS::removeVertex(BGPGraph *bgpg){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    if (vertex !=-1){
        bgpg->remove_vertex(vertex);
    }
    vertex = -1;
}

bool AS::update(bgpstream_pfx_t *pfx, unsigned int time){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    
    if (outage)
        outage = false;
    setObserved();
    if (activePrefixTrie->insert(pfx,NULL)){
        touch();
        inactivePrefixTrie->remove(pfx);
        BGPEvent *event = new BGPEvent(time, ASPREFA);
        event->map["asNum"] = to_myencoding(asNum);
        event->map["pfxID"] = to_myencodingPref(pfx);
        event->hash= std::hash<std::string>{}(event->map["asNum"]+":"+ event->map["pfxID"]);
        cache->bgpRedis->add(event);
        cTime = time;
        return true;
    } else {
        return false;
    }
}


bool AS::withdraw(bgpstream_pfx_t *pfx, unsigned int time){
   BGPEvent *event;
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    if( activePrefixTrie->remove(pfx)) {
        inactivePrefixTrie->insert(pfx,NULL);
        event = new BGPEvent(time, ASPREFW);
        event->map["asNum"] = to_myencoding(asNum);
        event->map["pfxID"] = to_myencodingPref(pfx);
        event->hash= std::hash<std::string>{}(event->map["asNum"]+":"+ event->map["pfxID"]);
        cache->bgpRedis->add(event);
        touch();
        if (activePrefixTrie->prefixNum()==0){
            outage = true;
            event = new BGPEvent(time, ASDROP);
            event->map["asNum"]=to_myencoding(asNum);
            event->hash= asNum;
            cache->bgpRedis->add(event);
            return true;
        }
    }
    return false;
}



void AS::addLink(unsigned long linkHash, unsigned int time){
    if (links.insert(linkHash)){
        touch();
        degree++;
        status &= ~DISCONNECTED;
    }
    if (time !=0)
        cTime = time;
    touched = true;
}

void AS::removeLink(unsigned long linkHash, unsigned int time){
//    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    if(links.erase(linkHash)){
        touch();
        degree--;
        if (links.size()==0){
            status |= DISCONNECTED;
        }
    }
    if (time !=0)
        cTime= time;
}


bool AS::checkLink(unsigned long linkHash){
    return !(links.find(linkHash)==links.end());
}

double AS::fusionRisks(){
    double stdRisk=1.0;
    if (stdRisk==0) {
        return 0;
    } else
        return secuRisk/(stdRisk*0.5+geoRisk*0.5);
}

Link* AS::findLink(unsigned long linkHash, BGPCache *cache){
    auto ret =links.find(linkHash);
    if (ret != links.end()){
        return cache->linksMap[*ret];
    } else {
        return NULL;
    }
        
}

void AS::clearLinks(){
    touch();
    links.clear();
}

bool AS::hasLinks(){
    return (links.size()>0);
}

void AS::toMap(std::unordered_map<std::string, std::string> &map){
    string str;
    map["ASN"] =to_myencoding(asNum);
//    map["NAM"]= name;

    vector<string> results;
    results.clear();
    boost::split(results, name, [](char c){return c == ':';});
    str +=results[0]+":";
    for (int i=1; i<results.size();i++){
        str +=":" + results[i] + ":";
    }
    //map["CNT"]= country;
    str +=country + ":";
    //map["RIR"]=RIR;
    str += RIR + ":";
    //map["OAD"]=ownerAddress;
    str += ownerAddress + ":";
    //map["LCD"]=lastChanged;
    str += lastChanged + ":";
    //map["DES"]=description;
    str +=description + ":";
    //map["DCR"]=dateCreated;
    str +=dateCreated + ":";
    //map["RSK"]=to_myencoding((int)10000*risk);
    str +=to_myencoding((int)10000*risk) + ":";
    //map["APN"]=to_myencoding(activePrefixNum);
    str +=to_myencoding(activePrefixNum) +":";
    //map["ALN"]=to_myencoding(allPrefixNum);
    str += to_myencoding(allPrefixNum)+":";
    //map["A24"]=to_myencoding(allPrefix24Num);
    str += to_myencoding(allPrefix24Num)+":";
    //map["ATN"]=to_myencoding(activePathsNum);
    str +=to_myencoding(activePathsNum) +":";
    //map["CTI"]=to_myencoding(cTime);
    str +=to_myencoding(cTime) +":";
    if (observed) {
        //map["OBS"]="T";
        str +="T:";
    } else {
        //map["OBS"]="F";
        str +="F:";
    }
    if (outage) {
        //map["OUT"]="T";
        str +="T:";
    } else {
//        map["OUT"]="F";
        str +="F:";
    }
    //map["STA"]=to_myencoding(status);
    str +=to_myencoding(status);
    map["STR"]=str;
}

void AS::fromRedis(string str){
    vector<string> results;
    results.clear();
    boost::split(results,str, [](char c){return c == ':';});
    int i=0;
    name="";
    while(true){
        if(results[i].empty()){
            name +=":";
        } else {
            name +=results[i];
            if (!results[i+1].empty())
                break;
        }
        i++;
    }
    i++;
    country=results[i++];
    RIR=results[i++];
    ownerAddress=results[i++];
    lastChanged=results[i++];
    description=results[i++];
    dateCreated=results[i++];
    risk = from_myencoding(results[i++])*1.0/100;
    activePrefixNum=from_myencoding(results[i++]);
    allPrefixNum=from_myencoding(results[i++]);
    allPrefix24Num=from_myencoding(results[i++]);
    activePathsNum=from_myencoding(results[i++]);
    cTime=from_myencoding(results[i++]);
    if (results[i++]=="T"){
        observed = true;
    } else {
        observed=false;
    }
    if (results[i++]=="T") {
        outage = true;
    } else {
        outage=false;
    }
    status=from_myencoding(results[i++]);
}

boost::graph_traits<Graph>::vertex_descriptor AS::getVertex(){
    return vertex;
}

Link::Link(unsigned int src, unsigned int dst, unsigned int time): src(src), dst(dst), cTime(time), bTime(time){
    srcAS = cache->chkAS(src, time);
    dstAS = cache->chkAS(dst, time);
    touch();
}

Link::Link(std::unordered_map<std::string, std::string> lMap){
    fromRedis(lMap["STR"]);
    srcAS = cache->chkAS(src, bTime);
    dstAS = cache->chkAS(dst, bTime);
    touch();
}

void Link::toRedis(std::unordered_map<std::string, std::string> &lMap){
    string str="";
    str +=to_myencoding(src)+":";
    //lMap["src"]=to_myencoding(src);
    str +=to_myencoding(dst)+":";
    //lMap["dst"]=to_myencoding(dst);
    str +=to_myencoding(bTime)+":";
    //lMap["BTI"]=to_myencoding(bTime);
    str +=to_myencoding(cTime)+":";
    //lMap["CTI"]=to_myencoding(cTime);
    if (active){
        str +="T:";
        //lMap["ACT"] = "T";
    } else {
        str +="F:";
        //lMap["ACT"] = "F";
    }
    str +=to_myencoding(pathNum);
//    lMap["PNU"]=to_myencoding(pathNum);
    lMap["LID"]=to_myencoding(src)+':'+to_myencoding(dst);
    lMap["STR"]=str;
}

void Link::fromRedis(string str){
    vector<string> results;
    results.clear();
    boost::split(results,str, [](char c){return c == ':';});
    int i=0;
    src=from_myencoding(results[i++]);
    dst=from_myencoding(results[i++]);
    bTime=from_myencoding(results[i++]);
    cTime=from_myencoding(results[i++]);
    if (results[i++]=="T"){
        active=true;
    } else {
        active=false;
    }
    pathNum=from_myencoding(results[i++]);

}

void Link::update(std::unordered_map<std::string, std::string> lMap){
    bTime=from_myencoding(lMap["BTI"]);
    cTime=from_myencoding(lMap["CTI"]);
    if (lMap["ACT"]=="T"){
        active=true;
    } else {
        active=false;
    }
    pathNum=from_myencoding(lMap["PNU"]);
    unsigned int time=bTime;
    srcAS = cache->chkAS(src, time);
    dstAS = cache->chkAS(dst, time);
    touch();
}

string Link::str(){
    return to_string(src)+"|"+to_string(dst);
}
int Link::size_of(){
    int size =14;
//    size += activePathsMap.size()*(4+8);
    return size;
}

void Link::addPath(unsigned int time){
    sem.acquire();
    touch();
    pathNum++;
    cTime = time;
    if (!active){
        active = true;
        addLinks(time);
        sem.release();
    }
    sem.release();
}

void Link::withdraw(unsigned int time){
    sem.acquire();
    touch();
    pathNum--;
    if (pathNum==0){
        if (active){
            active = false;
            removeLinks(time);
            BGPEvent *event = new BGPEvent(time, LINKDROP);
            event->map["LID"]= to_myencoding(src)+":"+to_myencoding(dst);
            event->hash= std::hash<std::string>{}(event->map["LID"]);
            cache->bgpRedis->add(event);
            sem.release();
        }
    }
    sem.release();
}


unsigned long Link::linkID(){
    unsigned long id=src;
    id=(id<<32)+dst;
    return id;
}

bool Link::isActive(){
    return active;
}
void Link::setActive(unsigned int time){
    addLinks(time);
    active= true;
}
void Link::unActive(unsigned int time){
    removeLinks(time);
    active= false;
}

bool Link::isTouched(){
    return touched;
}

void Link::touch(){
    if (!touched){
        cache->touchedLinks.push(this);
    }
    touched = true;
}

void Link::unTouch(){
    touched = false;
}

void Link::addLinks(unsigned int time){
    unsigned long linkid=linkID();
    srcAS->addLink(linkid, time);
    dstAS->addLink(linkid, time);
}

void Link::removeLinks(unsigned int time){
    unsigned long linkid=linkID();
    srcAS->removeLink(linkid, time);
    dstAS->removeLink(linkid, time);
}


void Link::checkEdge(BGPGraph *bgpg){
    srcAS->checkVertex(bgpg);
    dstAS->checkVertex(bgpg);
    bgpg->add_edge(srcAS->getVertex(), dstAS->getVertex(), EdgeP{(int)pathNum,0,0, 1, cTime});
}

void Link::removeEdge(BGPGraph *bgpg){
    bgpg->remove_edge(srcAS->getVertex(), dstAS->getVertex());
}
