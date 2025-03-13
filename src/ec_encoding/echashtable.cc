#include "ec_encoding/echashtable.h"
#include <iostream>

/*
    KV-EC-meta(primary):
        all_segment[  6bits  ][    14bits    ][   6bits   ][   6bits   ]    
                   [ chunkid ][     off      ][ parity1id ][ parity2id ] 

    KV-EC-meta(backup):
        all_segment[   8bits   ][   6bits  ][   12bits   ][   6bits   ]    
                   [    fp     ][ chunkid  ][     off    ][ parity1id ] 

    Stripe-meta:
    all_segment [                               64bits                                    ]
                [4bits][ 32bits ][3bits][  6bits  ][  6bits  ][  6bits  ][  6bits  ][1bits]
                [ WL  ][stripeid][  RL ][parity2id][parity3id][parity4id][parity5id][ MF  ]
*/

// add KvMeta

/*
    KV-EC-meta(backup):(kv_meta)
        all_segment[   8bits   ][   6bits  ][   12bits   ][   6bits   ]    
                   [    fp     ][ chunkid  ][     off    ][ parity1id ] 
*/

uint32_t get_kv_meta_stripe_id(KvMetaSlot *slot){
    return slot->stripe_id;
}

uint16_t get_kv_meta_fp(KvMetaSlot *slot){
    return read_bits(slot->all_segment, 0, 7);
}

uint16_t get_kv_meta_chunk_id(KvMetaSlot *slot){
    return read_bits(slot->all_segment, 8, 13);
}

uint16_t get_kv_meta_off(KvMetaSlot *slot){
    return read_bits(slot->all_segment, 14, 25);
}

uint16_t get_kv_meta_parity_rid(KvMetaSlot *slot){
    return read_bits(slot->all_segment, 26, 31);
}

void write_kv_meta_stripe_id(KvMetaSlot *slot, uint32_t value){
    slot->stripe_id = value;
}

void write_kv_meta_fp(KvMetaSlot *slot, uint16_t value){
    write_bits(&slot->all_segment, 0, 7, value);
}

void write_kv_meta_chunk_id(KvMetaSlot *slot, uint16_t value){
    write_bits(&slot->all_segment, 8, 13, value);
}

void write_kv_meta_off(KvMetaSlot *slot, uint16_t value){
    write_bits(&slot->all_segment, 14, 25, value);
}

void write_kv_meta_parity_rid(KvMetaSlot *slot, uint16_t value){
    write_bits(&slot->all_segment, 26, 31, value);
}

// add EcMeta

/*
    KV-EC-meta(primary):(ec_meta)
        all_segment[  6bits  ][    14bits    ][   6bits   ][   6bits   ]    
                   [ chunkid ][     off      ][ parity1id ][ parity2id ] 
*/

uint32_t get_ec_meta_stripe_id(EcMetaSlot *slot){
    return slot->stripe_id;
}

uint16_t get_ec_meta_chunk_id(EcMetaSlot *slot){
    return read_bits(slot->all_segment, 0, 5);
}

uint16_t get_ec_meta_off(EcMetaSlot *slot){
    return read_bits(slot->all_segment, 6, 19);
}

uint16_t get_ec_meta_parity_rid(EcMetaSlot *slot, uint8_t pid){

    uint16_t ret_value;

    switch(pid) {
        case 1:
            ret_value = read_bits(slot->all_segment, 20, 25);
            break;
        case 2:
            ret_value = read_bits(slot->all_segment, 26, 31);
            break;
        default:
            ret_value = -1;

    }
    return ret_value;
}

void write_ec_meta_stripe_id(EcMetaSlot *slot, uint32_t value){
    slot->stripe_id = value;
}

void write_ec_meta_chunk_id(EcMetaSlot *slot, uint16_t value){
    write_bits(&slot->all_segment, 0, 5, value);
}

void write_ec_meta_off(EcMetaSlot *slot, uint16_t value){
    write_bits(&slot->all_segment, 6, 19, value);
}

void write_ec_meta_parity_rid(EcMetaSlot *slot, uint16_t value, uint8_t pid){
    switch(pid) {
        case 1:
            write_bits(&slot->all_segment, 20, 25, value);
            break;
        case 2:
            write_bits(&slot->all_segment, 26, 31, value);
            break;
        default:
            printf("write kv meta parity rid pid illegal~\n");
            exit(0);
    }
}

// stripe meta

/*
    Stripe-meta:
    all_segment [                               64bits                                    ]
                [4bits][ 32bits ][3bits][  6bits  ][  6bits  ][  6bits  ][  6bits  ][1bits]
                [ WL  ][stripeid][  RL ][parity2id][parity3id][parity4id][parity5id][ MF  ]
*/

uint8_t get_stripe_meta_wl(StripeMetaSlot *stripe_meta_slot){
    return read_bits(stripe_meta_slot->all_segment, 0, 4);
}

uint32_t get_stripe_meta_stripe_id(StripeMetaSlot *stripe_meta_slot){
    return read_bits(stripe_meta_slot->all_segment, 5, 36);
}

uint8_t get_stripe_meta_rl(StripeMetaSlot *stripe_meta_slot){
    return read_bits(stripe_meta_slot->all_segment, 37, 38);
}

uint8_t get_stripe_meta_parity_rid(StripeMetaSlot *stripe_meta_slot, int parity_id){

    uint64_t all_segment = stripe_meta_slot->all_segment;

    switch(parity_id){
        case 1:
            return read_bits(all_segment, 39, 44);
        case 2:
            return read_bits(all_segment, 45, 50);
        case 3:
            return read_bits(all_segment, 51, 56);
        case 4:
            return read_bits(all_segment, 57, 62);
        default:
            return -1;
    }

    return -1;
}

uint8_t get_stripe_meta_mf(StripeMetaSlot *stripe_meta_slot){
    return read_bits(stripe_meta_slot->all_segment, 63, 63);
}

void write_stripe_meta_wl(StripeMetaSlot *stripe_meta_slot, uint8_t wl){
    write_bits(&stripe_meta_slot->all_segment, 0, 4, wl);
}

void write_stripe_meta_stripe_id(StripeMetaSlot *stripe_meta_slot, uint32_t stripe_id){
    write_bits(&stripe_meta_slot->all_segment, 5, 36, stripe_id);
}

void write_stripe_meta_rl(StripeMetaSlot *stripe_meta_slot, uint8_t rl){
    write_bits(&stripe_meta_slot->all_segment, 37, 38, rl);
}

void write_stripe_meta_parity_rid(StripeMetaSlot *stripe_meta_slot, int parity_id, uint8_t value){
    uint64_t *all_segment = &stripe_meta_slot->all_segment;

    switch(parity_id){
        case 1:
            write_bits(all_segment, 39, 44, value);
            break;
        case 2:
            write_bits(all_segment, 45, 50, value);
            break;
        case 3:
            write_bits(all_segment, 51, 56, value);
            break;
        case 4:
            write_bits(all_segment, 57, 62, value);
            break;
        default:
            cout << "error parity id~[" << parity_id << "]" << endl;
            exit(0);
            break;
    }
}

void write_stripe_meta_mf(StripeMetaSlot *stripe_meta_slot, uint8_t mf){
    write_bits(&stripe_meta_slot->all_segment, 63, 63, mf);
}

// other function
uint32_t get_free_stripe_slot_num(StripeMetaBucket * bucket, __OUT uint32_t * free_idx) { 
    *free_idx = RACE_HASH_ASSOC_NUM;
    uint32_t free_num = 0;
    for (int i = 0; i < RACE_HASH_ASSOC_NUM; i++) {
        if (bucket->slots[i].all_segment == 0) {
            free_num ++;
            *free_idx = i;
        }
    }
    return free_num;
}

void get_kv_meta_slot(KvMetaSlot *slot, KvEcMetaCtx *ctx){
    ctx->sid       = get_kv_meta_stripe_id(slot);
    ctx->fp        = get_kv_meta_fp(slot);
    ctx->cid       = get_kv_meta_chunk_id(slot);
    ctx->off       = get_kv_meta_off(slot);
    ctx->pid1      = get_kv_meta_parity_rid(slot);
}

void fill_kv_meta_slot(KvMetaSlot *slot, KvEcMetaCtx *ctx){
    write_kv_meta_stripe_id(slot, ctx->sid);
    write_kv_meta_fp(slot, ctx->fp);
    write_kv_meta_chunk_id(slot, ctx->cid);
    write_kv_meta_off(slot, ctx->off);
    write_kv_meta_parity_rid(slot, ctx->pid1);
}

void get_ec_meta_slot(EcMetaSlot *slot, KvEcMetaCtx *ctx){
    ctx->sid       = get_ec_meta_stripe_id(slot);
    ctx->cid       = get_ec_meta_chunk_id(slot);
    ctx->off       = get_ec_meta_off(slot);
    ctx->pid1      = get_ec_meta_parity_rid(slot, 1);
    ctx->pid2      = get_ec_meta_parity_rid(slot, 2);
}

void fill_ec_meta_slot(EcMetaSlot *slot, KvEcMetaCtx *ctx){
    write_ec_meta_stripe_id(slot, ctx->sid);
    write_ec_meta_chunk_id(slot, ctx->cid);
    write_ec_meta_off(slot, ctx->off);
    write_ec_meta_parity_rid(slot, ctx->pid1, 1);
    write_ec_meta_parity_rid(slot, ctx->pid2, 2);
}

void get_stripe_meta_slot(StripeMetaSlot *stripe_meta_slot, StripeMetaCtx *stripe_meta_ctx){
    stripe_meta_ctx->WL = get_stripe_meta_wl(stripe_meta_slot);
    stripe_meta_ctx->sid = get_stripe_meta_stripe_id(stripe_meta_slot);
    for(int i = 0;i < STRIPE_META_NUM_PID;i ++){
        stripe_meta_ctx->pid[i] = 
            get_stripe_meta_parity_rid(stripe_meta_slot, i + 1);
    }
    stripe_meta_ctx->RL = get_stripe_meta_rl(stripe_meta_slot);
    stripe_meta_ctx->MF = get_stripe_meta_mf(stripe_meta_slot);
}

void fill_stripe_meta_slot(StripeMetaSlot *slot, StripeMetaCtx *ctx){
    write_stripe_meta_wl(slot, ctx->WL);
    write_stripe_meta_stripe_id(slot, ctx->sid);
    write_stripe_meta_rl(slot, ctx->RL);
    for(int i = 0;i < STRIPE_META_NUM_PID;i ++){
        write_stripe_meta_parity_rid(slot, i + 1, ctx->pid[i]);
    }
    write_stripe_meta_mf(slot, ctx->MF);
}

void print_stripe_meta_ctx(StripeMetaCtx *stripe_meta_ctx){

    cout << "print stripe meta~" << endl;

    cout << "MF:" << (uint16_t)stripe_meta_ctx->MF << endl;
    cout << "RL:" << (uint16_t)stripe_meta_ctx->RL << endl;
    cout << "WL:" << (uint16_t)stripe_meta_ctx->WL << endl;

    cout << "stripe id:" << stripe_meta_ctx->sid << endl;
    cout << "parity id:" << endl;
    for(int i = 0;i < STRIPE_META_NUM_PID;i ++){
        cout << "parity [" << i << "] " << (uint16_t)stripe_meta_ctx->pid[i] << endl;
    }

    cout << "print stripe meta finished~" << endl;

}

int check_stripe_meta_wl(StripeMetaSlot *stripe_meta_slot, StripeMetaCtx *stripe_meta_ctx){

    if(stripe_meta_ctx->sid != get_stripe_meta_stripe_id(stripe_meta_slot)){
        return -1;
    }
    for(int i = 0;i < STRIPE_META_NUM_PID;i ++){
        if(stripe_meta_ctx->pid[i] != get_stripe_meta_parity_rid(stripe_meta_slot, i + 1)){
            return -1;
        }
    }
    if(stripe_meta_ctx->MF != get_stripe_meta_mf(stripe_meta_slot)){
        return -1;
    }
    if(stripe_meta_ctx->RL != get_stripe_meta_rl(stripe_meta_slot)){
        return -1;
    }

    return get_stripe_meta_wl(stripe_meta_slot);

}

int check_stripe_meta_rl(StripeMetaSlot *stripe_meta_slot, StripeMetaCtx *stripe_meta_ctx){

    if(stripe_meta_ctx->sid != get_stripe_meta_stripe_id(stripe_meta_slot)){
        return -1;
    }
    for(int i = 0;i < STRIPE_META_NUM_PID;i ++){
        if(stripe_meta_ctx->pid[i] != get_stripe_meta_parity_rid(stripe_meta_slot, i + 1)){
            return -1;
        }
    }
    if(stripe_meta_ctx->MF != get_stripe_meta_mf(stripe_meta_slot)){
        return -1;
    }
    if(stripe_meta_ctx->WL != get_stripe_meta_wl(stripe_meta_slot)){
        return -1;
    }

    return get_stripe_meta_rl(stripe_meta_slot);

}

uint32_t get_free_kv_ec_meta_slot_num_crash(KvEcMetaBucket * bucket, __OUT uint32_t * free_idx, uint8_t fp, int &slot_id) { 
    *free_idx = RACE_HASH_ASSOC_NUM;
    uint32_t free_num = 0;
    for (int i = 0; i < RACE_HASH_ASSOC_NUM; i++) {

        if(bucket->slots[i].kv_slot.fp == fp && bucket->slots[i].kv_slot.kv_len > 0){
            slot_id = i;
            return -2;
        }

        if (*(uint64_t *)&bucket->slots[i].ec_meta_slot == 0 && 
            *(uint64_t *)&bucket->slots[i].kv_slot == 0) {

            free_num ++;
            *free_idx = i;
        }
    }

    // cout<<"free_idx:"<<*free_idx<<endl;
    // cout<<"free num:"<<free_num<<endl;
    return free_num;
}

uint32_t get_free_kv_ec_meta_slot_num(KvEcMetaBucket * bucket, __OUT uint32_t * free_idx) { 
    *free_idx = RACE_HASH_ASSOC_NUM;
    uint32_t free_num = 0;
    for (int i = 0; i < RACE_HASH_ASSOC_NUM; i++) {

        if (*(uint64_t *)&bucket->slots[i].ec_meta_slot == 0 && 
            *(uint64_t *)&bucket->slots[i].kv_slot == 0) {

            free_num ++;
            *free_idx = i;
        }
    }

    // cout<<"free_idx:"<<*free_idx<<endl;
    // cout<<"free num:"<<free_num<<endl;
    return free_num;
}