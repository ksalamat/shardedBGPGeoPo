//
//  hash.h
//  BGPGeopol
//
//  Created by Kave Salamatian on 15/02/2020.
//  Copyright © 2020 Kave Salamatian. All rights reserved.
//

#ifndef hash_h
#define hash_h

// default values recommended by http://isthe.com/chongo/tech/comp/fnv/
const uint32_t Prime = 0x01000193; //   16777619
const uint32_t Seed  = 0x811C9DC5; // 2166136261
/// hash a single byte
inline uint32_t fnv1A(unsigned char oneByte, uint32_t hash = Seed)
{
  return (oneByte ^ hash) * Prime;
}

/// hash an std::string
uint32_t fnv1A(const std::string& str, uint32_t hash = Seed) {
    const char* text = str.c_str();
    while (*text)
      hash = fnv1A((unsigned char)*text++, hash);
    return hash ;
}

#endif /* hash_h */
