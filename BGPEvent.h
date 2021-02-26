//
//  BGPEvent.h
//  BGPGeo
//
//  Created by zhangxinyi on 2019/3/20.
//  Copyright © 2019年 zhangxinyi. All rights reserved.
//

#ifndef BGPEvent_h
#define BGPEvent_h
#include <unordered_map>
#include<string>
enum BGPEventType {PATHA=0, PATHW=1, NEWPATH=2, NEWAS=3, NEWLINK=4, NEWPREFIX=5, LINKDROP=6, ASDROP=7, TRIM=8, PATHAW=9, ASPREFA=10, ASPREFW=11, CAPTBEGIN=12, CAPTTIME=13, PATHACT=14,PATHNACT=15,ENDE=16,WITHDRAW=17, PATHAD=18, ASUPD=19, LNKUPD=20, PTHUPD=21};
using namespace std;

class BGPEvent{
public:
    unsigned int timestamp;
    BGPEventType eventType;
    std::unordered_map<string, string> map;
    unsigned int hash;
    BGPEvent(unsigned int time, BGPEventType event): timestamp(time), eventType(event){};
    ~BGPEvent(){
        map.clear();
    }
};


#endif /* BGPEvent_h */
