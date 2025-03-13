#ifndef METAEC_HASH_RING_TABLE_H
#define METAEC_HASH_RING_TABLE_H

#include "ec_encoding/hashring.h"
#include "core/hashtable.h"
#include<set>

#define MAX_COMBINED_SIZE 100
#define PLMAPLIST_SIZE sizeof(PLMapList)

struct PLMapList {
    uint8_t num_combined;
    uint8_t PL_map_element[MAX_COMBINED_SIZE][MAX_REP_NUM];
};

class HashRingTable {
    public:
        int k;
        int m;
        int num_ring;
        int num_mn;
        char mn_ip[16][16];
        HashRing *hashring;
        HashRootKvEcMeta *kv_ec_meta_hash_root_;
        ibv_mr *kv_ec_meta_hash_root_mr_;
        HashRootStripeMeta *stripe_meta_hash_root_;
        ibv_mr *stripe_meta_hash_root_mr_;
        HashRootPL *PL_hash_root_;
        ibv_mr *PL_hash_root_mr_;
        HashRootMetaData *metadata_hash_root_;
        ibv_mr *metadata_hash_root_mr_;
        map<uint16_t, uint8_t> *PL_map;
        PLMapList *PL_map_list_;
        ibv_mr *PL_map_list_mr_;
    public:
        HashRingTable();
        ~HashRingTable();
};

#endif