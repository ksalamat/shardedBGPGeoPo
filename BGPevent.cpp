//
// Created by Kave Salamatian on 2018-12-15.
//
#include "BGPevent.h"

void EventTable::checkOutage(AS *as, PrefixPeer *prefixPeer, unsigned int time){
    map<unsigned int, Events *>::iterator it;
    Events *eventsAS;
    OutageEvent *event; 
    unsigned  int asn = as->asNum;
    bool processed = false;

    it = eventASMap.find(asn);
    if (prefixPeer != NULL){
        if (it == eventASMap.end()){
            if ((as->checkOutage()) || (as->status && DISCONNECTED)) {
                //An outage have occured in asn  and an entry should be added
                eventsAS = new Events(asn);
                eventASMap.insert(pair<unsigned int, Events *>(asn, eventsAS));
            } else {
                //nothing to do !
                return;
            }
        } else {
            //there is already and entry for this AS
            eventsAS = it->second;
        }
        for (vector<Event *>::iterator it1 = eventsAS->activeEvents.begin(); it1 != eventsAS->activeEvents.end(); ++it1) {
            if ((*it1)->type == OUTAGE_EV) {
                OutageEvent *ev = (OutageEvent *) *it1;
                ev->addPrefix(prefixPeer);
                ev->involvedPrefixNum = ev->involvedPrefixSet.size();
                ev->allPrefixNum = as->allPrefixNum;
                processed = true;
                break;
            }
        }
        if (!processed) {
            event = (OutageEvent *) new Event(OUTAGE_EV, as, time);
            for(auto it1 = as->inactivePrefixSet.begin();it1!=as->inactivePrefixSet.end();it1++){
                event->addPrefix(*it1);
            }
            event->involvedPrefixNum = event->involvedPrefixSet.size();
            event->allPrefixNum = as->allPrefixNum;
            eventsAS->activeEvents.push_back(event);
        }
    } else {
        //this might be an end of event
        if (!(as->checkOutage()) && !(as->status & DISCONNECTED)) {
            //Check if the is really connected or out of outage
            if (it != eventASMap.end()) {
                eventsAS = it->second;
                for (vector<Event *>::iterator it1 = eventsAS->activeEvents.begin(); it1 != eventsAS->activeEvents.end(); ++it1) {
                    if ((*it1)->type == OUTAGE_EV) {
                        (*it1)->eTime = time;
                        if ((*it1)->eTime-(*it1)->bTime>300){
                            eventsAS->inactiveEvents.push_back(*it1);
                            cout<<"AS "<<asn<<" Outage begin: "<< (*it1)->bTime<<" end: " <<(*it1)->eTime << " involving "<<
                            (*it1)->involvedPrefixSet.size()<< " prefixes out of "<<as->allPrefixNum<<": ";
                            for (auto it2=(*it1)->involvedPrefixSet.begin();it2!=(*it1)->involvedPrefixSet.end(); it2++){
                                cout<<(*it2)->str()<<",";
                            }
                            cout<<endl;
                        }
                        eventsAS->activeEvents.erase(it1);
                        break;
                    }
                }
            }
        }
    }
}

void EventTable::checkHijack(AS *as, PrefixPeer *prefixPeer, unsigned int time) {
    Events *eventsAS;
    HijackEvent *event;
    bool processed = false;
    int asn1 = as->asNum, asn;
    AS *hijackedAS, *hijackingAS = as;
    map<unsigned int, Events *>::iterator it1;

    if (prefixPeer->asSet.size() > 1) {
        //hijack begin
        for (auto it = prefixPeer->asSet.begin(); it != prefixPeer->asSet.end(); it++) {
            if (*it != asn1) {
                asn = *it; //hijacked AS
                it1 = eventASMap.find(asn);
                if (it1 == eventASMap.end()) {
                    if (as->status & HIJACKING) {
                        //An hijack have occured  and an entry should be added
                        eventsAS = new Events(asn);
                        eventASMap.insert(pair<unsigned int, Events *>(asn, eventsAS));
                    } else {
                        //nothing to do !
                        return;
                    }
                } else {
                    //there is already and entry for this AS
                    eventsAS = it1->second;
                }
                for (vector<Event *>::iterator it1 = eventsAS->activeEvents.begin();
                     it1 != eventsAS->activeEvents.end(); ++it1) {
                    if ((*it1)->type == HIJACK_EV) {
                        HijackEvent *ev = (HijackEvent *) *it1;
                        ev->addPrefix(prefixPeer);
                        ev->involvedPrefixNum = ev->involvedPrefixSet.size();
                        ev->allPrefixNum = as->allPrefixNum;
                        processed = true;
                        break;
                    }
                }
                if (!processed) {
                    event = (HijackEvent *) new HijackEvent(as, asn, time);
                    event->addPrefix(prefixPeer);
                    event->involvedPrefixNum = event->involvedPrefixSet.size();
                    event->allPrefixNum = as->allPrefixNum;
                    eventsAS->activeEvents.push_back(event);
                }
            }
        }
    } else {
        //this might be an end of hijack
        for (auto it = prefixPeer->asSet.begin(); it != prefixPeer->asSet.end(); it++) {
            asn = *it; //hijacked AS
            it1 = eventASMap.find(asn);
            if (it1 != eventASMap.end()) {
                eventsAS = it1->second;
                if (!(as->status & HIJACKING)) {
                    for (auto it2 = eventsAS->activeEvents.begin(); it2 != eventsAS->activeEvents.end(); ++it2) {
                        if ((*it2)->type == HIJACK_EV) {
                            HijackEvent *hijackEvent = (HijackEvent *) *it2;
                            hijackEvent->eTime = time;
                            if (hijackEvent->eTime - hijackEvent->bTime > 300) {
                                eventsAS->inactiveEvents.push_back(hijackEvent);
                                cout << "AS " << asn << " Hijack begin: " << hijackEvent->bTime << " end: "
                                     << hijackEvent->eTime
                                     << " involving " << hijackEvent->involvedPrefixSet.size() << " prefixes:";
                                for (auto it3 = hijackEvent->involvedPrefixSet.begin();
                                     it3 != hijackEvent->involvedPrefixSet.end(); it3++) {
                                    cout << (*it3)->str() << ",";
                                }
                                cout << endl;
                                cout << "Hijacked AS: " << asn << " Hijacking AS: " << hijackEvent->asn << endl;
                            }
//                            it2->active= false;
                            break;
                        }
                    }
                }
            }
        }
    }
}

Event::Event(EventType type, AS *as, unsigned int time): type(type), as(as), bTime(time),asn(as->asNum), active(true){}


