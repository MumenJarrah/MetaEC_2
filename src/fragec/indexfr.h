#ifndef METAEC_INDEXFR_
#define METAEC_INDEXFR_

#include<stdint.h>

#define MAX_OBJECT_INDEX (1024 * 1024 * 64)
#define MAX_STRIPE_INDEX (1024 * 256)
#define MAX_CHUNKE_INDEX (1024 * 256)
#define OBJ_INDEX_LEN sizeof(ObjectIndex)
#define STR_INDEX_LEN sizeof(StripeIndex)
#define CHU_INDEX_LEN sizeof(ChunkIndex)
#define MAX_OBJ_LEN (MAX_OBJECT_INDEX * OBJ_INDEX_LEN)
#define MAX_STR_LEN (MAX_STRIPE_INDEX * STR_INDEX_LEN)
#define MAX_CHU_LEN (MAX_CHUNKE_INDEX * CHU_INDEX_LEN)

struct ObjectIndex{
    uint32_t stripe_id;
    uint8_t length;
    uint8_t offset;
    uint8_t chunk_index;
    uint8_t fp;
};

struct PositionMessage{
    uint8_t server_id;
    uint64_t addr;
};

struct StripeIndex{
    int cid[4]; // all chunk id
    PositionMessage pmsg[2];
};

struct ChunkIndex{
    void* obj_addr;
    PositionMessage pmsg;
    void* next;
};

#endif