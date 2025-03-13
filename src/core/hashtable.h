#ifndef DDCKV_HASH_TABLE_H_
#define DDCKV_HASH_TABLE_H_

#include "kv_utils.h"
#define RACE_HASH_GLOBAL_DEPTH              (5)
#define RACE_HASH_INIT_LOCAL_DEPTH          (5)
#define RACE_HASH_SUBTABLE_NUM              (1 << RACE_HASH_GLOBAL_DEPTH)
#define RACE_HASH_INIT_SUBTABLE_NUM         (1 << RACE_HASH_INIT_LOCAL_DEPTH)
#define RACE_HASH_MAX_GLOBAL_DEPTH          (5)
#define RACE_HASH_MAX_SUBTABLE_NUM          (1 << RACE_HASH_MAX_GLOBAL_DEPTH)
#define RACE_HASH_ADDRESSABLE_BUCKET_NUM    (1024 * 256)
#define RACE_HASH_SUBTABLE_BUCKET_NUM       (RACE_HASH_ADDRESSABLE_BUCKET_NUM * 3 / 2)
#define SUBTABLE_USED_HASH_BIT_NUM          (32)
#define RACE_HASH_MASK(n)                   ((1 << n) - 1)
#define RACE_HASH_MAX_SLOT (RACE_HASH_INIT_SUBTABLE_NUM * RACE_HASH_ADDRESSABLE_BUCKET_NUM * RACE_HASH_ASSOC_NUM)
#define ROOT_RES_LEN          (sizeof(RaceHashRoot))
#define SUBTABLE_LEN          (RACE_HASH_ADDRESSABLE_BUCKET_NUM * sizeof(RaceHashBucket))
#define SUBTABLE_RES_LEN      (RACE_HASH_MAX_SUBTABLE_NUM * SUBTABLE_LEN)
#define META_AREA_LEN         (256 * 1024 * 1024)
#define GC_AREA_LEN           (0)
#define HASH_AREA_LEN         (1024 * 1024 * 1024)
#define LOG_AREA_LEN          (128 * 1024 * 1024)
#define CLIENT_META_LEN       (1 * 1024 * 1024)
#define CLIENT_GC_LEN         (1 * 1024 * 1024)
#define RACE_BUCKET_SIZE      sizeof(RaceHashBucket)
#define COMBINED_BUCKET_SIZE  2 * sizeof(RaceHashBucket)
#define KV_BUCKET_SIZE        sizeof(KvEcMetaBucket)
#define MAX_SERVER            (10)
#define MAX_RANGE             (10)
#define MAX_M                 4
#define MAX_ROOT_ENTRY        100
#define MAX_HASH_RING         10
#define ROOT_KV_META_LEN      (sizeof(HashRootKvEcMeta))
#define ROOT_STRIPE_META_LEN  (sizeof(HashRootStripeMeta))
#define ROOT_PL_LEN           (sizeof(HashRootPL))
#define ROOT_METADATA_LEN     (sizeof(HashRootMetaData))
#define MAX_SMALL_SUBTABLE 100
#define MAX_BIG_SUBTABLE 20
#define BIG_SUBTABLE_LEN    (RACE_HASH_ADDRESSABLE_BUCKET_NUM * sizeof(KvEcMetaBucket))

typedef struct __attribute__((__packed__)) TagRaceHashSlot {
    uint8_t fp;
    uint8_t kv_len;
    uint8_t server_id;
    uint8_t pointer[5];
} RaceHashSlot;

typedef struct __attribute__((__packed__)) TagRacsHashBucket {
    uint32_t local_depth;
    uint32_t prefix;
    RaceHashSlot slots[RACE_HASH_ASSOC_NUM];
} RaceHashBucket;

typedef struct TagRaceHashSubtableEntry {
    uint8_t lock;
    uint8_t local_depth;
    uint8_t server_id;
    uint8_t pointer[5];
} RaceHashSubtableEntry;

typedef struct TagSubTableEntryKvEcMeta {
    RaceHashSubtableEntry subtable_entry_rep[MAX_M];
    RaceHashSubtableEntry subtable_entry_primary;
} SubTableEntryKvEcMeta;

typedef struct TagHashRootKvEcMeta {
    SubTableEntryKvEcMeta subtable_entry[MAX_SERVER][MAX_RANGE];
} HashRootKvEcMeta;

typedef struct TagHashRootStripeMeta {
    SubTableEntryKvEcMeta subtable_entry[MAX_SERVER][MAX_RANGE];
} HashRootStripeMeta;

typedef struct TagSubTableEntryPL {
    RaceHashSubtableEntry subtable_entry_PL[MAX_M];
} SubTableEntryPL;

typedef struct TagHashRootPL {
    SubTableEntryPL subtable_entry[MAX_ROOT_ENTRY];
} HashRootPL;

typedef struct TagSubTableEntryMetaData {
    RaceHashSubtableEntry subtable_entry_metadata[MAX_HASH_RING];
} SubTableEntryMetaData;

typedef struct TagHashRootMetaData {
    SubTableEntryMetaData subtable_entry[MAX_ROOT_ENTRY];
} HashRootMetaData;

typedef struct TagRaceHashRoot {
    uint64_t global_depth;
    uint64_t init_local_depth;
    uint64_t max_global_depth;
    uint64_t prefix_num;
    uint64_t subtable_res_num;
    uint64_t subtable_init_num;
    uint64_t subtable_hash_num;
    uint64_t subtable_hash_range;
    uint64_t subtable_bucket_num;
    uint64_t seed;
    uint64_t mem_id;
    uint64_t root_offset;
    uint64_t subtable_offset;
    uint64_t kv_offset;
    uint64_t kv_len;
    uint64_t lock;
    RaceHashSubtableEntry subtable_entry[RACE_HASH_MAX_SUBTABLE_NUM][MAX_REP_NUM];
} RaceHashRoot;

static inline uint64_t subtable_index(uint64_t hash_value, uint64_t capacity){
    return hash_value % capacity;
}

static inline uint64_t subtable_first_index(uint64_t hash_value, uint64_t capacity) {
    return hash_value % (capacity / 2);
}

static inline uint64_t subtable_second_index(uint64_t hash_value, uint64_t f_index, uint64_t capacity) {
    uint32_t hash = hash_value;
    uint16_t partial = (uint16_t)(hash >> 16);
    uint16_t non_sero_tag = (partial >> 1 << 1) + 1;
    uint64_t hash_of_tag = (uint64_t)(non_sero_tag * 0xc6a4a7935bd1e995);
    return (uint64_t)(((uint64_t)(f_index) ^ hash_of_tag) % (capacity / 3) + capacity / 3);
}

static inline uint64_t hash_index_convert_40_to_64_bits(uint8_t * addr) {
    uint64_t ret = 0;
    return ret | ((uint64_t)addr[0] << 32) | ((uint64_t)addr[1] << 24)
        | ((uint64_t)addr[2] << 16) | ((uint64_t)addr[3] << 8) 
        | ((uint64_t)addr[4] << 0);
}

static inline void hash_index_convert_64_to_40_bits(uint64_t addr, __OUT uint8_t * o_addr) {
    o_addr[0] = (uint8_t)((addr >> 32) & 0xFF);
    o_addr[1] = (uint8_t)((addr >> 24) & 0xFF);
    o_addr[2] = (uint8_t)((addr >> 16) & 0xFF);
    o_addr[3] = (uint8_t)((addr >> 8) & 0xFF);
    o_addr[4] = (uint8_t)((addr >> 0)  & 0xFF);
}

static inline void convert_slot_to_addr(RaceHashSlot * slot, __OUT KVRWAddr * kv_rw_addr) {
    kv_rw_addr->server_id = slot->server_id;
    kv_rw_addr->r_kv_addr = hash_index_convert_40_to_64_bits(slot->pointer);
}

static inline uint64_t convert_slot_to_int(RaceHashSlot * slot) {
    return *(uint64_t *)slot;
}

uint64_t VariableLengthHash(const void * data, uint64_t length, uint64_t seed);
uint8_t  hash_index_compute_fp(uint64_t hash);
uint32_t get_free_slot_num(RaceHashBucket * bucekt, __OUT uint32_t * free_idx);
bool IsEmptyPointer(uint8_t * pointer, uint32_t num);
bool check_key(void * r_key_addr, uint32_t r_key_len, void * l_key_addr, uint32_t l_key_len);

static inline uint64_t hash_64int_to_64int(uint64_t value) {
    value = (~value) + (value << 21); 
    value = value ^ (value >> 24);
    value = (value + (value << 3)) + (value << 8); 
    value = value ^ (value >> 14);
    value = (value + (value << 2)) + (value << 4);
    value = value ^ (value >> 28);
    value = value + (value << 31);
    return value;
}

static inline uint8_t get_kv_slot_server(RaceHashSlot *slot){
    return slot->server_id;
}

static inline uint8_t get_kv_slot_len(RaceHashSlot *slot){
    return slot->kv_len;
}

#endif