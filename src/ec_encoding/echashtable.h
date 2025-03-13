#ifndef DDCKV_EC_HASH_TABLE_H
#define DDCKV_EC_HASH_TABLE_H

#include <core/kv_utils.h>
#include "core/hashtable.h"
// add metadata RACE hash

#define STRIPE_META_NUM_PID 4

typedef struct TagKvEcMetaCtx {
    uint32_t sid;
    uint16_t fp;
    uint16_t cid;
    uint16_t off;
    uint16_t pid1;
    uint16_t pid2;
}KvEcMetaCtx;

typedef struct TagStripeMetaCtx {
    uint8_t WL;
    uint32_t sid;
    uint8_t RL;
    uint8_t pid[STRIPE_META_NUM_PID];
    uint8_t MF;
}StripeMetaCtx;

// kv ec meta replication(include fp)
typedef struct __attribute__((__packed__)) TagKVMetaSlot {
    uint32_t all_segment;
    uint32_t stripe_id;
} KvMetaSlot;

// kv ec meta primary(not include fp)
typedef struct __attribute__((__packed__)) TagEcMetaSlot {
    uint32_t all_segment;
    uint32_t stripe_id;
} EcMetaSlot;

// primary kv(include kv slot and primary kv ec meta)
typedef struct __attribute__((__packed__)) TagKvEcMetaSlot {
    RaceHashSlot kv_slot;
    EcMetaSlot   ec_meta_slot;
} KvEcMetaSlot;

// stripe meta slot 
typedef struct __attribute__((__packed__)) TagStripeMetaSlot {
    uint64_t all_segment;
} StripeMetaSlot;

// add metadata RACE hash
typedef struct __attribute__((__packed__)) TagKVMetaBucket {
    uint32_t local_depth;
    uint32_t prefix;
    KvMetaSlot slots[RACE_HASH_ASSOC_NUM];
} KvMetaBucket;

typedef struct __attribute__((__packed__)) TagKvEcMetaBucket {

    uint64_t local_depth;
    uint64_t prefix;
    KvEcMetaSlot slots[RACE_HASH_ASSOC_NUM];

} KvEcMetaBucket;

typedef struct __attribute__((__packed__)) TagStripeMetaBucket {
    uint32_t local_depth;
    uint32_t prefix;
    StripeMetaSlot slots[RACE_HASH_ASSOC_NUM];
} StripeMetaBucket;

static inline uint64_t convert_stripe_slot_to_int(StripeMetaSlot * slot) {
    return *(uint64_t *)slot;
}

static inline uint64_t convert_kv_slot_to_int(KvMetaSlot * slot) {
    return *(uint64_t *)slot;
}

// add KvMeta
uint32_t get_kv_meta_stripe_id(KvMetaSlot *slot);
uint16_t get_kv_meta_fp(KvMetaSlot *slot);
uint16_t get_kv_meta_chunk_id(KvMetaSlot *slot);
uint16_t get_kv_meta_off(KvMetaSlot *slot);
uint16_t get_kv_meta_parity_rid(KvMetaSlot *slot);
void write_kv_meta_stripe_id(KvMetaSlot *slot, uint32_t value);
void write_kv_meta_fp(KvMetaSlot *slot, uint16_t value);
void write_kv_meta_chunk_id(KvMetaSlot *slot, uint16_t value);
void write_kv_meta_off(KvMetaSlot *slot, uint16_t value);
void write_kv_meta_parity_rid(KvMetaSlot *slot, uint16_t value);

// add EcMeta
uint32_t get_ec_meta_stripe_id(EcMetaSlot *slot);
uint16_t get_ec_meta_chunk_id(EcMetaSlot *slot);
uint16_t get_ec_meta_off(EcMetaSlot *slot);
uint16_t get_ec_meta_parity_rid(EcMetaSlot *slot, uint8_t pid);
void write_ec_meta_stripe_id(EcMetaSlot *slot, uint32_t value);
void write_ec_meta_chunk_id(EcMetaSlot *slot, uint16_t value);
void write_ec_meta_off(EcMetaSlot *slot, uint16_t value);
void write_ec_meta_parity_rid(EcMetaSlot *slot, uint16_t value, uint8_t pid);

// add StripeMeta
uint8_t get_stripe_meta_wl(StripeMetaSlot *stripe_meta_slot);
uint32_t get_stripe_meta_stripe_id(StripeMetaSlot *stripe_meta_slot);
uint8_t get_stripe_meta_rl(StripeMetaSlot *stripe_meta_slot);
uint8_t get_stripe_meta_parity_rid(StripeMetaSlot *stripe_meta_slot, int parity_id);
uint8_t get_stripe_meta_mf(StripeMetaSlot *stripe_meta_slot);

void write_stripe_meta_wl(StripeMetaSlot *stripe_meta_slot, uint8_t wl);
void write_stripe_meta_stripe_id(StripeMetaSlot *stripe_meta_slot, uint32_t stripe_id);
void write_stripe_meta_rl(StripeMetaSlot *stripe_meta_slot, uint8_t rl);
void write_stripe_meta_parity_rid(StripeMetaSlot *stripe_meta_slot, int parity_id, uint8_t value);
void write_stripe_meta_mf(StripeMetaSlot *stripe_meta_slot, uint8_t mf);

// other function
void get_kv_meta_slot(KvMetaSlot *slot, KvEcMetaCtx *ctx);
void fill_kv_meta_slot(KvMetaSlot *slot, KvEcMetaCtx *ctx);
void get_ec_meta_slot(EcMetaSlot *slot, KvEcMetaCtx *ctx);
void fill_ec_meta_slot(EcMetaSlot *slot, KvEcMetaCtx *ctx);
void get_stripe_meta_slot(StripeMetaSlot *slot, StripeMetaCtx *ctx);
void fill_stripe_meta_slot(StripeMetaSlot *slot, StripeMetaCtx *ctx);

void print_stripe_meta_ctx(StripeMetaCtx *stripe_meta_ctx);

int check_stripe_meta_wl(StripeMetaSlot *stripe_meta_slot, StripeMetaCtx *stripe_meta_ctx);
int check_stripe_meta_rl(StripeMetaSlot *stripe_meta_slot, StripeMetaCtx *stripe_meta_ctx);

uint32_t get_free_stripe_slot_num(StripeMetaBucket * bucket, __OUT uint32_t * free_idx);
uint32_t get_free_kv_ec_meta_slot_num(KvEcMetaBucket * bucket, __OUT uint32_t * free_idx);
uint32_t get_free_kv_ec_meta_slot_num_crash(KvEcMetaBucket * bucket, __OUT uint32_t * free_idx, uint8_t fp, int &slot_id);

#endif