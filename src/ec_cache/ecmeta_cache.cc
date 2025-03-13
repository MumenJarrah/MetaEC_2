#include "ec_cache/ecmeta_cache.h"

EcMetaCache::EcMetaCache(size_t size){
    cache_capacity = size * M_SIZE;
    
    cache_map = new unordered_map<string, KvMessage*>;

    // kv_message_queue = new queue <KvMessage *>;

    // for(int i = 0;i < MAX_KV;i ++){
    //     KvMessage *kv_message = new KvMessage;
    //     *(uint64_t *)kv_message = 0;
    //     kv_message_queue->push(kv_message);
    // }
}

EcMetaCache::~EcMetaCache(){
    delete(cache_map);
}

void EcMetaCache::insert_cache(string key, RaceHashSlot kv_slot, KvMetaSlot kv_meta_slot,
    int kv_slot_count, int kv_meta_count, int kv_slot_miss_count, int kv_meta_miss_count){

    // KvMessage *kv_message = kv_message_queue->front();
    // kv_message_queue->pop();
    KvMessage *kv_message = new KvMessage;
    kv_message->kv_slot = kv_slot;
    kv_message->kv_meta_slot = kv_meta_slot;
    kv_message->kv_slot_count = kv_slot_count;
    kv_message->kv_slot_miss_count = kv_slot_miss_count;
    kv_message->kv_meta_count = kv_meta_count;
    kv_message->kv_meta_miss_count = kv_meta_miss_count;

    // auto it = cache_map->find(key);

    // if(it != cache_map->end()){
    //     cache_map->erase(key);
    // }

    // cache_map->insert(make_pair(key, kv_message));
    (*cache_map)[key] = kv_message;

}

bool EcMetaCache::check_kv_message(string key, KvMessage & kv_message){

    KvMessage *ret;

    auto it = cache_map->find(key);

    if(it == cache_map->end()){
        return false;
    } else {
        ret = it->second;
        kv_message = *ret;
        return true;
    }

}

bool EcMetaCache::check_kv_slot(string key, RaceHashSlot & kv_slot){
    // KvMessage *ret;
    // bool if_find = ec_meta_cache->get(key, ret);
    // // cout << "if_find:" << if_find << "ret:" << ret << endl;
    // if(if_find == false){
    //     return false;
    // } else {
    //     kv_slot = ret->kv_slot;
    //     if(*(uint64_t *)&kv_slot == 0){
    //         return false;
    //     } else {
    //         return true;
    //     }
    // }
}

bool EcMetaCache::check_kv_meta_slot(string key, KvMetaSlot & kv_meta_slot){
    // KvMessage *ret;
    // bool if_find = ec_meta_cache->get(key, ret);
    // // cout << "ret:" << ret << endl;
    // if(if_find == false){
    //     return false;
    // } else {
    //     kv_meta_slot = ret->kv_meta_slot;
    //     if(*(uint64_t *)&kv_meta_slot == 0){
    //         return false;
    //     } else {
    //         return true;
    //     }
    // }
}

RaceHashSlot *EcMetaCache::get_kv_slot(string key){
    // KvMessage *ret = ec_meta_cache->get(key);
    // if(ret == NULL){
    //     return NULL;
    // } else {
    //     return &ret->kv_slot;
    // }
    return NULL;
}

KvMetaSlot *EcMetaCache::get_kv_meta_slot(string key){
    // KvMessage *ret = ec_meta_cache->get(key);
    // if(ret == NULL){
    //     return NULL;
    // } else {
    //     return &ret->kv_meta_slot;
    // }
    return NULL;
}

// bool EcMetaCache::check_key(string key){
//     return ec_meta_cache->exists(key);
// }

// size_t EcMetaCache::get_cache_capacity(){
//     return cache_capacity;
// }

// size_t EcMetaCache::get_cache_size(){
//     return ec_meta_cache->size();
// }

void EcMetaCache::print_cache(){
    // auto cache_list = ec_meta_cache->get_list();
    // string key;
    // RaceHashSlot kv_slot;
    // KVMetaSlot   kv_meta;
    // for(auto it = cache_list.begin();it != cache_list.end();it ++){
    //     key = it->first;
    //     kv_slot = it->second->kv_slot;
    //     kv_meta = it->second->kv_meta_slot;

    //     RDMA_LOG(3) << "key: " << key << " stripe:" << kv_meta.stripe_id; 
    // }
}

void EcMetaCache::remove_cache(string key){
    cache_map->erase(key);
}   

// void EcMetaCache::get_cache_miss_rate(){
//     RDMA_LOG(3) << "miss rate:" << ec_meta_cache->get_miss_rate();
// }