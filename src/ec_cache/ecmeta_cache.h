#ifndef METAEC_ECMETA_CACHE
#define METAEC_ECMETA_CACHE

#include "ec_cache/lrucache.hpp"
#include "ec_encoding/echashtable.h"
#include "core/hashtable.h"
#include "core/logging.hpp"
#include <queue>
#include <map>

#define MAX_KV  1000000
#define M_SIZE 1024 * 1024

using namespace std;

struct KvMessage{
    RaceHashSlot kv_slot;
    KvMetaSlot   kv_meta_slot;
    int kv_slot_count;
    int kv_meta_count;
    int kv_slot_miss_count;
    int kv_meta_miss_count;
};

class EcMetaCache{

    public:
    
        EcMetaCache(size_t size);
        ~EcMetaCache();

        void insert_cache(string key, RaceHashSlot kv_slot, KvMetaSlot kv_meta_slot, 
            int kv_slot_count, int kv_meta_count, int kv_slot_miss_count, int kv_meta_miss_count);
        RaceHashSlot *get_kv_slot(string key);
        KvMetaSlot   *get_kv_meta_slot(string key);
        bool check_kv_slot(string key, RaceHashSlot & kv_slot);
        bool check_kv_meta_slot(string key, KvMetaSlot & kv_meta_slot);
        bool check_kv_message(string key, KvMessage & kv_message);
        void remove_cache(string key);
        bool check_key(string key);

        size_t get_cache_capacity();
        size_t get_cache_size();
        void print_cache();

        void get_cache_miss_rate();

    private:
        size_t cache_capacity;
        unordered_map<string, KvMessage*> *cache_map;

        queue <KvMessage *> *kv_message_queue;

};

#endif