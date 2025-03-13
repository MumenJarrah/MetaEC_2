#ifndef DDCKV_SERVER_MM_
#define DDCKV_SERVER_MM_

#include "core/config.h"
#include "core/hashtable.h"
#include "ec_encoding/echashtable.h"

#include <unordered_map>
#include <queue>

class ServerMM {
private:
    uint16_t my_sid_;
    uint64_t base_addr_;
    uint64_t base_len_; 
    uint64_t client_meta_area_off_;
    uint64_t client_meta_area_len_;
    uint64_t client_gc_area_off_;
    uint64_t client_gc_area_len_;
    uint64_t client_hash_area_off_;
    uint64_t client_hash_area_len_;
    uint64_t client_log_area_off_;
    uint64_t client_log_area_len_;
    uint64_t client_kv_area_off_;
    uint64_t client_kv_area_len_;
    uint64_t client_kv_area_limit_;
    uint32_t num_memory_;
    uint32_t num_replication_; 
    uint32_t allocate_size;
    uint32_t num_blocks_; 
    struct ibv_mr  * mr_;
    bool if_print_log;
    uint64_t kv_ec_meta_root_addr;
    uint64_t stripe_meta_root_addr;
    uint64_t PL_root_addr;
    uint64_t metadata_root_addr;
    uint64_t small_subtable_addr;
    uint64_t big_subtable_addr;
    int max_small_subtable;
    int max_big_subtable;
#ifdef SERVER_MM
    uint64_t next_free_block_addr_;
#endif
    queue<uint64_t> allocable_blocks_;
    unordered_map<uint64_t, bool> allocated_blocks_; 
    void   * data_; 
    vector<bool> small_subtable_alloc_map_; 
    vector<bool> big_subtable_alloc_map_;
    vector<bool> log_alloc_map_; 
private:
    void get_allocable_blocks();
    int init_root(void * root_addr);
    int init_subtable();
    int init_hashtable();
    int init_log();
public:
    ServerMM(uint64_t server_base_addr, uint64_t base_len, 
        uint32_t block_size, const struct IbInfo * ib_info,
        const struct GlobalConfig * conf);
    ~ServerMM();
    uint64_t mm_alloc();
    uint64_t mm_alloc_small_subtable();
    uint64_t mm_alloc_big_subtable();
    uint64_t mm_alloc_log(uint8_t client_id);
    int mm_free(uint64_t st_addr);
    uint32_t get_rkey();
    int get_client_gc_info(uint32_t client_id, __OUT struct MrInfo * mr_info);
    int get_mr_info(__OUT struct MrInfo * mr_info);
    uint64_t get_kv_ec_meta_addr();
    uint64_t get_stripe_meta_addr();
    uint64_t get_PL_addr();
    uint64_t get_metadata_addr();
    uint64_t get_subtable_addr();
    void print_hashtable_size();
};
#endif