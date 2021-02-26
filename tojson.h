//
// Created by Kave Salamatian on 2018-12-23.
//

#ifndef BGPGEOPOLITICS_TOJSON_H
#define BGPGEOPOLITICS_TOJSON_H
#include "json.hpp"
#include "BGPevent.h"

using json = nlohmann::json;
void to_json(json& j, const OutageEvent& p) {
    vector<string> pfxStrings;
    for (auto it2=p.involvedPrefixSet.begin();it2!=p.involvedPrefixSet.end(); it2++){
        pfxStrings.push_back((*it2)->str);
    }
    j = json{{"asn", p.asn}, {"bTime", p.bTime}, {"eTime", p.eTime}, {"type", p.type}, {"pfxs", pfxStrings}};
}


#endif //BGPGEOPOLITICS_TOJSON_H
