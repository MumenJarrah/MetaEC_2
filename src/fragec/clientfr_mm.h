#ifndef DDCKV_CLIENTFR_MM_H_
#define DDCKV_CLIENTFR_MM_H_

#include "core/nm.h"
#include "core/hashtable.h"
#include "core/logging.hpp"

#include <queue>

#define MAX_NUM_SUBBLOCKS 4
#define MAX_WATER_MARK 0.7

typedef struct TagClientMMBlock {
    struct MrInfo mr_info_list;
    uint8_t server_id_list;
} ClientMMBlock;

enum ClientAllocType {
    TYPE_SMALL_SUBTABLE = 1,
    TYPE_KVBLOCK  = 2,
    TYPE_LOG      = 3,
    TYPE_BIG_SUBTABLE = 0
};

typedef struct TagClientMMAllocCtx { 
    uint32_t num_subblocks;
    uint32_t rkey_list;
    uint64_t addr_list;
    uint8_t  server_id_list;
} ClientMMAllocCtx;

typedef struct TagClientMMAllocSubtableCtx {
    uint8_t  server_id;
    uint64_t addr;
} ClientMMAllocSubtableCtx;

typedef struct TagClientMMAllocLogCtx {
    uint8_t  server_id;
    uint64_t addr;
} ClientMMAllocLogCtx;

typedef struct TagSubblockInfo {
    uint64_t addr_list;
    uint32_t rkey_list;
    uint8_t  server_id_list;
} SubblockInfo;

enum {
    INIT_KV,
    KV_BLOCK_TYPE,
    ENCODING_BLOCK

};

class ClientFRMM {
private:
    uint32_t num_memory_;
    int client_id;
    uint32_t subblock_num_;
    uint32_t last_allocated_;
    uint32_t bmap_block_num_;
    uint64_t server_limit_addr_; 
    uint64_t server_kv_area_addr_;
    uint64_t server_num_blocks_;

    // modification
    deque<SubblockInfo> *subblock_free_queue_;
    deque<SubblockInfo> *subblock_free_queue_encoding_;

    struct timeval local_recover_space_et_;
    struct timeval get_addr_meta_et_;
    struct timeval traverse_log_et_;

    int init_get_new_block_from_server(UDPNetworkManager * nm);
    int init_reg_space(struct MrInfo mr_inf_list[], uint8_t server_id_list[],
            UDPNetworkManager * nm, int reg_type);
    int dyn_get_new_block_from_server(UDPNetworkManager * nm, uint64_t server_id, int is_enc);
    int local_reg_blocks(const struct MrInfo * mr_info_list, const uint8_t * server_id_list, int is_enc);
    int reg_new_space(const struct MrInfo * mr_info_list, const uint8_t * server_id_list,
            UDPNetworkManager * nm, int reg_type);
    int dyn_reg_new_space(const struct MrInfo * mr_info_list, const uint8_t * server_id_list,
            UDPNetworkManager * nm, int reg_type, int is_enc);
    int32_t alloc_from_sid(uint32_t server_id, UDPNetworkManager * nm, int alloc_type,
            __OUT struct MrInfo * mr_info);

    inline uint32_t get_alloc_hint_rr() {
        return last_allocated_ ++;
    }

public:
    uint64_t mm_block_sz_;
    uint64_t subblock_sz_;

    ClientFRMM(const struct GlobalConfig * conf, UDPNetworkManager * nm);
    ~ClientFRMM();
    
    void mm_alloc(size_t size, UDPNetworkManager * nm, uint64_t server_id, __OUT ClientMMAllocCtx * ctx);
    void mm_alloc_encoding(size_t size, UDPNetworkManager * nm, uint64_t server_id, __OUT ClientMMAllocCtx * ctx);
    void mm_alloc_small_subtable(UDPNetworkManager * nm, __OUT ClientMMAllocSubtableCtx * ctx, uint8_t server_id);
    void mm_alloc_big_subtable(UDPNetworkManager * nm, __OUT ClientMMAllocSubtableCtx * ctx, uint8_t server_id);
    void mm_alloc_log(UDPNetworkManager * nm, __OUT ClientMMAllocLogCtx * ctx);

    void get_time_bread_down(std::vector<struct timeval> & time_vec);
    void check_subblock();
    void print_leave_block();

    inline size_t get_aligned_size(size_t size) { 
        if ((size % subblock_sz_) == 0) {
            return size;
        }
        size_t aligned = ((size / subblock_sz_) + 1) * subblock_sz_;
        return aligned;
    }

};

#endif