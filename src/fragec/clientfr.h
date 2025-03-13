#ifndef DDCKV_CLIENTFR_H_
#define DDCKV_CLIENTFR_H_

#include "core/hashtable.h"
// #include "ec_encoding/hashring.h"
#include "ec_encoding/city_hash.h"
#include "ec_encoding/echashtable.h"
#include "core/thread_poll.h"
#include "core/logging.hpp"
#include "ec_cache/ecmeta_cache.h"
#include <chrono>
#include "ec_encoding/buffer.h"
#include "ec_encoding/rscoding.h"
#include "ec_log/ec_log.h"
#include "core/Timer.h"
#include "ec_encoding/hashringtable.h"
#include "core/combination.h"
#include "fragec/indexfr.h"
#include "fragec/clientfr_mm.h"

using namespace std;
using Clock = chrono::high_resolution_clock;
using TimePoint = chrono::time_point<Clock>;

#define CLINET_INPUT_BUF_LEN  (512 * 1024 * 1024ULL)
// #define CLINET_INPUT_BUF_LEN  (512 * 1024 * 1024ULL * 2)
#define CLINET_METADATA_BUF   (30 * 16384ULL)
#define CLIENT_PARITY_BUF     (10 * 16384ULL)
#define CLIENT_KV_META_BUCKET_BUFFER  (32 * 1024 * 1024)
#define CORO_LOCAL_BUF_LEN    (32 * 1024 * 1024)
#define CLIENT_ENCODING_BUCKET_LEN  (64 * 1024 * 1024)

#define MAX_TEST_NUM 1000000

#define READ_BUCKET_ST_WRID         100
#define WRITE_KV_ST_WRID            200
#define READ_KV_ST_WRID             300
#define CAS_ST_WRID                 400
#define INVALID_ST_WRID             500
#define FAA_ST_WRID                 600
#define READ_PARITY_LOGGING_BUCKET  700
#define WRITE_PARITY_LOGGING        800
#define READ_STRIPE_META_BUCKET     900
#define READ_EC_META_BUCKET         1000
#define READ_P0_BUCKET              1100
#define READ_LOGGING_BUCKET         1200
#define READ_KV_BUCKET              1300

#define READ_P0                     1500
#define READ_LOGGING                1600
#define CAS_PL_bucket               1700
#define READ_CACHE_KV               1800
#define WRITE_PARITY_CHUNK          1900
#define WRITE_METADATA_CHUNK        2000
#define CAS_PARITY_BUCKET           2100

#define READ_PARITY_CHUNK_BUCKET    2400
#define UPDATE_READ_OBJECT_INDEX    2500
#define UPDATE_READ_STRIPE_INDEX    2600
#define UPDATE_READ_PARITY_CHUNK    2700
#define UPDATE_WRITE_PARITY_CHUNK   2800
#define UPDATE_WRITE_STRIPE_INDEX   2900
#define UPDATE_CHAIN_WALKING        2930
#define DELETE_READ_OBJECT_INDEX    3000
#define DELETE_READ_STRIPE_INDEX    3100
#define DELETE_READ_PARITY_CHUNK    3200
#define DELETE_WRITE_PARITY_CHUNK   3300
#define DELETE_WRITE_STRIPE_INDEX   3400
#define DELETE_REMOVE_OBJECT_INDEX  3500

#define DEGRADE_READ_OBJECT_INDEX   3600
#define DEGRADE_READ_STRIPE_INDEX   3610
#define DEGRADE_READ_PARITY_CHUNK   3620
#define DEGRADE_READ_CHUNK_INDEX    3630
#define DEGRADE_READ_RELATED_KV     3640

#define ENCODING_WRITE_STRIPE_INDEX 3800
#define ENCODING_WRITE_OBJECT_INDEX 3900
#define ENCODING_WRITE_CHUNK_INDEX  3950
#define UPDATE_STRIPE_META_REREAD   4000
#define BASELINE_WRITE_LOG_ENTRY    4100
#define BASELINE_WRITE_INVALID      4200
#define UPDATE_LOG_END              4300
#define UPDATE_WRITE_PL_BUCKET      4400
#define UPDATE_RECOVERY_READ_SLOT   4500
#define DELETE_CAS_PL_SLOT          4600
#define DELETE_WRITE_PL_SLOT        4700
#define DELETE_WRITE_KV_META        4800
#define DELETE_LOG_END              4900

#define READ_KV                     10000
#define READ_KV_META_BUCKET         40000
#define WRITE_KV_SLOT        65000

#define MAX_TEST 10000000

#define MAX_SR_LIST 200000
#define MAX_ENCODING_SR_LIST 5000

#define REMOVE_RDMA_FIND_TIMES 2
#define SERVER_CORO_RANGE 1024 * 4
#define NUM_UPDATE_RTT 5

#define CAS_SIZE sizeof(uint64_t)
#define FAA_SIZE sizeof(uint64_t)

#define BASELINE_LOG_SIZE (sizeof(uint64_t) + sizeof(uint8_t))
#define BASELINE_INVALID_SIZE 1

#define MAX_COUNT_SIZE 1024 * 1024
#define RAND_RECOVERY_TIMES 10
#define CAS_FAILED_RECOVERY_TIMES 10

#define KV_ALL_USDE_SLOT (RACE_HASH_ASSOC_NUM * 2 + 1)

#define RECOVERY_BUF_LEN    (256 * 1024)
#define RECOVERY_MN_BUF_LEN (32 * 1024)

// #define UPDATE_SEND_TYPE_LATENCY_LENGTH 1024 * 1024

enum rep_type {
    KV_REP,
    STRIPE_META_REP
};

enum KVRequestType {
    KV_REQ_SEARCH,
    KV_REQ_INSERT,
    KV_REQ_UPDATE,
    KV_REQ_DELETE,
    KV_REQ_RECOVER_COMMITTED,
    KV_REQ_RECOVER_UNCOMMITTED,
    KV_DEGRADE_READ,
    KV_MERGE_PL
};

enum KVOpsRetCode {
    KV_OPS_SUCCESS = 0,
    KV_OPS_FAIL_RETURN,
    KV_OPS_FAIL_REDO
};

struct NumCount{

    int update_RTT2_read_stripe_meta_failed;
    int update_RTT2_read_wl_is_15;
    int update_RTT3_faa_return_invalied;
    int update_RTT3_cas_return_invalied;
    int update_RTT4_cas_kv_failed;

    int update_recovery_kv_num;
    int update_find_kv_failed;
    int update_match_kv_failed;

    int search_find_failed;
    int search_match_failed;

    int cache_hit;
    int cache_miss;
    int cache_hit_but_failed;

    int num_search;
    int num_update;
    int num_insert;
    int num_degrade;
};

struct UpdateRTTLatency {

    double RTT1;
    double RTT2;
    double RTT3;
    double RTT4;
    double RTT5;

};

enum UpdateSendType {

    RTT1_SEND_KV,
    RTT2_KV,
    RTT2_STRIPE,
    RTT3_STRIPE,
    RTT3_KV,
    RTT3_PL_1,
    RTT3_PL_2,
    RTT4_KV,
    RTT4_PL_1,
    RTT4_PL_2,
    RTT5_STRIPE,
    UST_COUNT

};

struct UpdateServerLatency {
    uint8_t server_id;
    double  latency;
};

struct BucketSlotIdx {
    int bucket_idx;
    int slot_idx;
    uint64_t slot_value;
};

struct BucketBuffer {
    RaceHashBucket * local_bucket_addr;  
    void           * local_cache_addr;   
    void           * local_kv_addr;      
    void           * local_cas_target_value_addr;   
    void           * local_cas_return_value_addr;    
    void           * op_laddr;    

    // bucket_info
    RaceHashBucket * bucket_arr[4];
    RaceHashBucket * f_com_bucket;
    RaceHashBucket * s_com_bucket;
    RaceHashSlot   * slot_arr[4]; 

    // for insert
    int32_t bucket_idx;
    int32_t slot_idx;

    std::vector<KVCASAddr> kv_modify_pr_cas_list;
};

typedef struct TagParityLogging{
    uint16_t len;
    uint16_t off;
    uint8_t crc;
}ParityLogging;

// degrade read workflow:find kv message from ec metadata
typedef struct TagDegradeReadNeedKv{
    int num_off; // Which is the first kV in the list of all kV
    int num_kv;  // How many kv does this metadata require in total
    int first_off;  // the offset of the first kv
    int first_len;  // the length of the first kv
    int end_off;    // the offset of the end kv
    uint64_t metadata_addr; // the addr of this metadata
}NeedKv;
typedef struct TagUpdateCtx{
    
    uint64_t object_index_addr;
    uint64_t stripe_index_addr;
    uint64_t *parity_chunk_addr;

    uint32_t stripe_id;

}UpdateCtx;

typedef struct TagDeleteCtx {

    uint64_t object_index_addr;
    uint64_t stripe_index_addr;
    uint64_t *parity_chunk_addr;

    uint32_t stripe_id;

    uint64_t object_0_addr;
    
} DeleteCtx;


typedef struct TagDegradeReadCtx {
    uint64_t object_index_addr;
    uint64_t stripe_index_addr;
    uint64_t *parity_chunk_addr;

    uint32_t stripe_id;
    uint64_t *related_kv_addr;

    u8 **data;

}DegradeReadCtx;

typedef struct TagEncodingCtx {

    RsCoding *rs_coding;
    u8 **parity_chunk;

    void *buf_;
    ibv_mr *buf_mr_;

    void *parity_buf;
    void *bucket_buf;
    void *map_buf;

    KVInfo *parity_info;
    KVHashInfo *parity_hash_info;
    KVTableAddrInfo *parity_addr_info;
    BucketBuffer *parity_bucket_buffer;

    ClientMMAllocCtx *mm_alloc_parity_ctx;

    StripeMetaCtx stripe_meta_ctx;

    KvEcMetaCtx **kv_meta_ctx;

    vector<KvBuffer> *kv_message;
    EncodingMessage  *en_message;

    vector<int> data_chunk_id;
    vector<int> parity_chunk_id;

    int stripe_id;

    bool if_encoding;

    vector<IbvSrList *> sr_list_batch;
    vector<uint32_t>    sr_list_num_batch;
    map<uint64_t, bool> comp_wrid_map;

    int encoding_metadata_size;

    int kv_alloc_size;
    int block_size;

    int num_idx_rep;

    int predict_num_kv_meta;

    int *free_log_num;
    uint64_t *return_faa_kv_log;

    uint64_t *stripe_index;
    uint64_t local_bucket_addr;
    uint64_t *local_object_addr;
    uint64_t *local_chunk_addr;
    uint64_t slot_return_addr;
    uint64_t slot_target_addr;

    // add pre malloc sr list 
    // sr list malloc 
    CircularQueue<IbvSrList *> *sr_list_queue;
    CircularQueue<ibv_send_wr *> *sr_queue;
    CircularQueue<ibv_sge *> *sge_queue;

    CircularQueue<ibv_send_wr *> *double_sr_queue;
    CircularQueue<ibv_sge *>   *double_sge_queue;


    // add for hashring
    vector<uint8_t>  *num_mn;
    vector<uint8_t>  *mn_index;

} EncodingCtx;

typedef struct TagKVReqCtx {
    // input
    uint64_t         coro_id;
    uint8_t          req_type;
    KVInfo         * kv_info;
    RaceHashBucket * local_bucket_addr;  // need to be allocated to each coro ib_mr
    void           * local_cache_addr;   // need to be allocated to each coro ib_mr
    void           * local_kv_addr;      // need to be allocated to each coro ib_mr
    void           * local_cas_target_value_addr;     // need to be allocated to each coro ib_mr
    void           * local_cas_return_value_addr;     // need to be allocated to each coro ib_mr
    void           * op_laddr;    // tmp results
    uint32_t         lkey;
    bool             use_cache;

    // bucket_info
    RaceHashBucket * bucket_arr[4];
    RaceHashBucket * f_com_bucket;
    RaceHashBucket * s_com_bucket;
    RaceHashSlot   * slot_arr[4]; 

    // for preparation
    KVHashInfo      hash_info;
    KVTableAddrInfo tbl_addr_info;
    
    // for key-value pair read
    std::vector<KVRWAddr>                     kv_read_addr_list;
    std::vector<std::pair<int32_t, int32_t> > kv_idx_list;

    // for kv insert/update/delete
    ClientMMAllocCtx mm_alloc_ctx;
    uint8_t consensus_state;
    std::vector<KVCASAddr> kv_modify_pr_cas_list;

    // for insert
    int32_t bucket_idx;
    int32_t slot_idx;

    // for kv update/delete
    KVRWAddr kv_invalid_addr;
    std::vector<std::pair<int32_t, int32_t> > recover_match_idx_list;

    // on crash
    std::vector<uint8_t> healthy_idx_server_id_list;    // record the replication idx of the healthy index servers

    bool is_finished;
    bool is_local_cache_hit;
    bool committed_need_cas_pr;
    bool has_modified_bk_idx;
    bool failed_pr_index; // set when the primary index failed
    bool failed_pr_addr;  // set when the primary data failed
    union {
        void * value_addr;    // for search return value
        int    ret_code;
    } ret_val;

    volatile bool * should_stop;

    std::string key_str;

    uint64_t kv_all_len;

    // add update workflow
    UpdateCtx *uctx;

    // add delete workflow
    DeleteCtx *dctx;

    // add degrade read workflow
    DegradeReadCtx *drctx;

    IbvSrList *sr_list_cache;

    // add log
    ClientMMAllocCtx insert_kv_log_mm_alloc_ctx;
    LogEntry *insert_kv_log_buf_;
    void *insert_faa_wp_return_addr;

    // add comp wr id
    map<uint64_t, bool> comp_wrid_map;

    // cache get
    KvMessage* kv_message;

    int pre_ctx_index;

    uint64_t old_value;

    bool if_batch;

    vector<IbvSrList *> sr_list_batch;
    vector<uint32_t>    sr_list_num_batch;

    int ctx_id;

} KVReqCtx;

typedef struct TagCountCtx{

    int index;
    int count[MAX_COUNT_SIZE];

} CountCtx;

typedef struct TagRecoveryCtx{
    KVReqCtx *kv_req_ctx;
    KVInfo *kv_info;
    std::vector<IbvSrList *> sr_list_batch;
    std::vector<uint32_t> sr_list_num_batch;
    struct ibv_wc wc;
    void *rec_buf_;
    struct ibv_mr *rec_buf_mr_;
    uint64_t *server_buf_addr;
    int *server_num_log;
    int num_op;
    int num_common_op;
    int num_failed_op;

} RecoveryCtx;

struct MemoryConsumption{
    uint64_t kv_data;
    uint64_t parity_data;
    uint64_t kv_ec_meta;
    uint64_t stripe_meta;
    uint64_t metadatablock;
    uint64_t log;
};

class ClientFR {
private:

    UDPNetworkManager * nm_;
    ClientFRMM * mm_;

    uint32_t client_id;

    uint64_t pr_log_tail_;

    uint64_t remote_global_meta_addr_;
    uint64_t remote_sync_addr;

    uint64_t remote_PL_map_addr;
 
    uint64_t remote_root_addr_;

    // remote hash table root addr
    uint64_t remote_kv_ec_meta_root_addr_;
    uint64_t remote_object_index_addr;
    uint64_t remote_stripe_index_addr;
    uint64_t remote_chunk_index_addr;

    uint64_t server_st_addr_;
    uint64_t server_data_len_;

    void * local_buf_;
    struct ibv_mr * local_buf_mr_;

    void * input_buf_;
    struct ibv_mr * input_buf_mr_;
    void * insert_ecmeta_buf;
    void * insert_parity_buf;
    void * kv_meta_bucket_buf;

    // add update workflow as parity logging buf

    uint64_t * coro_local_addr_list_;

    std::map<uint32_t, struct MrInfo *> server_mr_info_map_;

    // RACE hash
    RaceHashRoot * race_root_;
    struct ibv_mr * race_root_mr_;

    // Parity logging hash
    RaceHashRoot * race_root_PL_;
    struct ibv_mr * race_root_PL_mr_;

    // stripe meta and ec meta 
    RaceHashRoot * race_root_meta_;
    struct ibv_mr * race_root_meta_mr_;

    // core bind information
    uint32_t main_core_id_;
    uint32_t poll_core_id_;
    uint32_t encoding_core_id_;

    // hash ring
    HashRing *hashring;

    // hash ring table
    HashRingTable *hashring_table;

    // thread
    pthread_t polling_tid;

    struct ibv_mr * frag_ptr_mr_;
    struct ibv_mr ** ec_meta_ptr_mr_;

    bool mm_alloc_mutex;

    // add log
    ClientLog *client_log;

    // sr list malloc 
    CircularQueue<IbvSrList *> *sr_list_queue;
    CircularQueue<ibv_send_wr *> *sr_queue;
    CircularQueue<ibv_sge *> *sge_queue;

    CircularQueue<ibv_send_wr *> *double_sr_queue;
    CircularQueue<ibv_sge *>   *double_sge_queue;

    float miss_rate_threash;

    bool if_batch;

// private inline methods
private:

    inline int poll_completion(std::map<uint64_t, bool> & wait_wrid_wc_map){
        int ret = 0;
        while (nm_->is_all_complete(wait_wrid_wc_map) == false) {
            boost::this_fiber::sleep_for(std::chrono::microseconds(10));
            ret = nm_->nm_check_completion(wait_wrid_wc_map);
        }
        return ret;
    }

    inline int poll_completion_thread(std::map<uint64_t, bool> & wait_wrid_wc_map){
        int ret = 0;
        while (nm_->is_all_complete(wait_wrid_wc_map) == false) {
            ret = nm_->nm_check_completion(wait_wrid_wc_map);
        }
        return ret;
    }
 
// public methods
public:

    int *count_server;
    int *count_used_kv_slot;
    int num_count[6];
    ClientFR(struct GlobalConfig * conf);
    ~ClientFR();
    bool check_if_encoding;

    // some test config
    KVInfo   * kv_info_list_;
    KVReqCtx * kv_req_ctx_list_;
    uint32_t   num_total_operations_;
    uint32_t   num_local_operations_;
    float      workload_run_time_;
    uint32_t   num_coroutines_; 
    uint32_t num_memory_;
    int all_clients;
    int max_stripe;
    int base_stripe_id;
    int pre_stripe;
    int num_cn;

    // kv relation
    int key_size;
    int value_size;
    int k_m;

    // check if print log or not
    bool if_print_log;

    /*
        this is insert workflow need some struct
    */

    int wr_id_stripe_meta;

    // thread poll
    // ThreadPool p1;

    int num_test;

    // add generate update trace
    deque<string> *update_trace_key_list_per_stripe;

    // add generate delete trace
    deque<string> *delete_trace_key_list_per_stripe;

    // add generate degrade read trace
    deque<string> *degrade_read_trace_key_list_per_stripe;

    // add buffer struct
    Buffer *buffer;

    // add test
    NumCount code_num_count;

    // CountCtx countctx;

    void kv_insert_buffer(KVReqCtx * ctx);

    // add pre malloc sr list
    void init_sr_list();
    IbvSrList *get_one_sr_list();
    ibv_send_wr *get_one_sr();
    ibv_sge *get_one_sge();
    ibv_send_wr *get_double_sr();
    ibv_sge *get_double_sge();

    IbvSrList *get_one_sr_list_encoding();
    ibv_send_wr *get_one_sr_encoding();
    ibv_sge *get_one_sge_encoding();
    ibv_send_wr *get_double_sr_encoding();
    ibv_sge *get_double_sge_encoding();

    // add encoding struct
    EncodingCtx *ectx;
    void init_encoding_mm_space();
    void free_encoding_mm_space();
    void free_sr_list(IbvSrList * sr_list, int num_sr_lists);
    void make_trace();
    void init_trace();

    // add thread pool
    ThreadPool encoding_thread;
    void encoding_wait();
    void encoding_check();
    void encoding_check_make_trace();

    void encoding_prepare_sync();
    void encoding_make_parity_metadata();
    void encoding_prepare_parity();

    // add encoding change log pointer
    void encoding_init_sr_list();
    void encoding_finish();
    void encoding_finish_trace();
    void encoding_and_make_stripe_sync();
    void encoding_and_make_stripe_sync_trace();

    // add fragec
    void encoding_stripe_parity();
        void encoding_write_stripe_index();
        void encoding_write_parity_chunk();
    void encoding_kv();
        void encoding_kv_RTT1(KvBuffer *kv_buffer);
            void encoding_read_kv_bucket_and_write_kv(KvBuffer *kv_buffer);
        void encoding_kv_RTT2(KvBuffer *kv_buffer);
            void encoding_cas_kv_slot(KvBuffer *kv_buffer, uint64_t *origin_value);
        void encoding_ec_metadata(KvBuffer *kv_buffer, KvEcMetaCtx *kv_meta_ctx, int obj_status, 
            int chunk_id, uint64_t *alloc_addr);
            void encoding_write_object_index(KvBuffer *kv_buffer, KvEcMetaCtx *kv_meta_ctx);
            void encoding_write_chunk_index(KvBuffer *kv_buffer, int obj_status, int chunk_id, 
                uint64_t *alloc_addr, uint64_t mm_alloc_addr);

    IbvSrList *gen_one_sr_list(uint64_t local_addr, uint32_t local_len, uint32_t lkey, 
        uint32_t coro_id, uint8_t server_id, uint32_t type, uint32_t wr_id_offset, 
        ibv_wr_opcode opcode, uint64_t remote_addr, int send_flags, ibv_send_wr *next);

    IbvSrList *gen_one_cas_sr_list(uint64_t local_addr, uint32_t lkey, uint32_t coro_id, uint8_t server_id,
        uint32_t type, uint32_t wr_id_offset, uint64_t remote_addr, uint64_t origin_value, 
        uint64_t new_value, int send_flags, ibv_send_wr *next);

    // add cache
    EcMetaCache *ec_meta_cache;
    bool is_use_cache;

    // kv_req relation
    int load_kv_req_micro_latency(int num_op, const char * op);
    int load_kv_req_from_file_ycsb(const char * fname, uint32_t st_idx, int32_t num_ops);
    int load_kv_req_file_mncrashed(const char * fname, uint32_t st_idx, int32_t num_ops);
    int load_kv_req(int num_op, const char * op);
    void load_kv_req_init_update(KVReqCtx *ctx);
    void load_kv_req_init_delete(KVReqCtx *ctx);
    void load_kv_req_init_degrade_read(KVReqCtx * ctx);
    void init_kv_req_ctx(KVReqCtx * ctx, char * operation);
    void init_kv_req_ctx_ycsb(KVReqCtx * ctx, char * operation);

    // kv_op
    int kv_insert_sync(KVReqCtx * ctx);
    void *kv_search_sync(KVReqCtx * ctx);

    void *kv_search(KVReqCtx *ctx);
        void kv_search_read_buckets_async(KVReqCtx * ctx);
        void kv_search_read_kv_async(KVReqCtx * ctx);

    int kv_update_sync(KVReqCtx * ctx);
        void kv_update_RTT1_sync(KVReqCtx *ctx);
            void kv_update_read_bucket_and_write_kv(KVReqCtx *ctx);
            void kv_update_read_object_index(KVReqCtx *ctx);
        void kv_update_RTT2_sync(KVReqCtx *ctx);
            void kv_update_read_kv(KVReqCtx *ctx);
            void kv_update_read_stripe_index(KVReqCtx *ctx);
        void kv_update_RTT3_sync(KVReqCtx *ctx);
            void kv_update_read_parity_chunk(KVReqCtx *ctx);
        void kv_update_RTT4_sync(KVReqCtx *ctx);
            void kv_update_encoding_parity(KVReqCtx *ctx);
            void kv_update_write_parity(KVReqCtx *ctx);
            void kv_update_write_stripe_index_chain_walking(KVReqCtx *ctx);
        void kv_update_RTT5_sync(KVReqCtx *ctx);
            void kv_update_cas_slot(KVReqCtx *ctx);

    int kv_update_async(KVReqCtx * ctx);
        void kv_update_RTT1_async(KVReqCtx *ctx);
        void kv_update_RTT2_async(KVReqCtx *ctx);
        void kv_update_RTT3_async(KVReqCtx *ctx);
        void kv_update_RTT4_async(KVReqCtx *ctx);
        void kv_update_RTT5_async(KVReqCtx *ctx);

    int kv_delete_sync(KVReqCtx * ctx);
        void kv_delete_RTT1_sync(KVReqCtx *ctx);
            void kv_delete_read_bucket(KVReqCtx *ctx);
            void kv_delete_read_object_index(KVReqCtx *ctx);
        void kv_delete_RTT2_sync(KVReqCtx *ctx);
            void kv_delete_read_kv(KVReqCtx *ctx);
            void kv_delete_read_stripe_index(KVReqCtx *ctx);
            void kv_delete_remove_object_index(KVReqCtx *ctx);
        void kv_delete_RTT3_sync(KVReqCtx *ctx);
            void kv_delete_read_parity_chunk(KVReqCtx *ctx);
        void kv_delete_RTT4_sync(KVReqCtx *ctx);
            void kv_delete_remove_parity(KVReqCtx *ctx);
            void kv_delete_write_parity(KVReqCtx *ctx);
            void kv_delete_write_stripe_index(KVReqCtx *ctx);
        void kv_delete_RTT5_sync(KVReqCtx *ctx);
            void kv_delete_cas_slot(KVReqCtx *ctx);

    void *kv_degread_sync(KVReqCtx *ctx);
        void kv_degrade_RTT1(KVReqCtx *ctx);
            void kv_degrade_read_object_index(KVReqCtx *ctx);
        void kv_degrade_RTT2(KVReqCtx *ctx);
            void kv_degrade_read_stripe_index(KVReqCtx *ctx);
        void kv_degrade_RTT3(KVReqCtx *ctx);
            void kv_degrade_read_parity_chunk(KVReqCtx *ctx);
        void kv_degrade_RTT4(KVReqCtx *ctx);
            void kv_degrade_get_kv_chain_walking(KVReqCtx *ctx);
        void kv_degrade_RTT5(KVReqCtx *ctx);
            void kv_degrade_decoding(KVReqCtx *ctx);

    void *kv_degrade_read(KVReqCtx *ctx);
        void kv_degrade_RTT1_async(KVReqCtx *ctx);
        void kv_degrade_RTT2_async(KVReqCtx *ctx);
        void kv_degrade_RTT3_async(KVReqCtx *ctx);
        void kv_degrade_RTT4_async(KVReqCtx *ctx);
            void kv_degrade_get_kv_chain_walking_async(KVReqCtx *ctx);

// private methods
private:
    bool init_is_finished();
    int  sync_init_finish();
    int  connect_ib_qps();

    void init_all_hash_table_mem();
    void read_all_hash_table();
    void write_all_hash_table();
    int init_all_hash_index();
    int init_all_hash_subtable();

    void mm_alloc_to_subtable_entry(RaceHashSubtableEntry & entry, ClientMMAllocSubtableCtx & mmctx);

    // kv_req relation
    void prepare_request(KVReqCtx * ctx);
    void get_kv_addr_info(KVHashInfo * hash_info, __OUT KVTableAddrInfo * addr_info);
    void get_kv_hash_info(KVInfo * kv_info, __OUT KVHashInfo * hash_info);
    void get_bucket_index(KVHashInfo * hash_info, KVTableAddrInfo *addr_info);

    // degrade second workflow: get ec meta bucket;get stripe meta to get wl,rl;get P0 and its loggings bucket
    void hash_compute_fp(KVHashInfo * hash_info);
    // hash_value -> server id(m + 1);subtable start addr(m + 1);subtable bucket offset(bucket id)
    void get_server_subtable_kv(KVHashInfo * hash_info, KVTableAddrInfo *addr_info);
    void get_server_subtable_parity(KVHashInfo * hash_info, KVTableAddrInfo *addr_info, int bufferid);
    void get_rep_server_subtable_addr(KVHashInfo * hash_info, KVTableAddrInfo *addr_info, uint8_t type);
    
    // kv relation
    IbvSrList * gen_write_kv_sr_lists(uint32_t coro_id, KVInfo * a_kv_info, 
        ClientMMAllocCtx * r_mm_info, __OUT uint32_t * num_sr_lists, int id);
    IbvSrList * gen_write_kv_sr_lists_encoding(uint32_t coro_id, KVInfo * a_kv_info,    
        ClientMMAllocCtx * r_mm_info, __OUT uint32_t * num_sr_lists, int id);
    IbvSrList * gen_read_kv_sr_lists(uint32_t coro_id, const std::vector<KVRWAddr> & r_addr_list, 
        __OUT uint32_t * num_sr_lists);

    void kv_search_read_buckets_sync(KVReqCtx * ctx);
    void kv_search_read_kv_sync(KVReqCtx * ctx);
    void kv_search_check_kv(KVReqCtx * ctx);
    void get_local_bucket_info_kv(KVReqCtx * ctx);
    void find_kv_slot(KVReqCtx * ctx);
    bool find_kv_slot_in_buckets(KvEcMetaBucket *bucket, KVRWAddr *rw, 
        pair<int32_t, int32_t> *pair, uint8_t fp, uint64_t local_addr, int op_type);
    int32_t find_match_kv_idx(KVReqCtx * ctx);

    void fill_slot(ClientMMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info,
        __OUT RaceHashSlot * local_slot);

// for testing
public:

    // EC message
    int k;
    int m;

    void init_kvreq_space(uint32_t coro_id, uint32_t kv_req_st_idx, uint32_t num_ops);
    void init_kv_insert_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_insert_space(void * coro_local_addr, KVReqCtx * ctx);
    void init_kv_search_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_search_space(void * coro_local_addr, KVReqCtx * ctx);
    void init_kv_update_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_update_space(void * coro_local_addr, KVReqCtx * ctx);
    void init_kv_delete_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_delete_space(void * coro_local_addr, KVReqCtx * ctx);
    void init_kv_degrade_read_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_degrade_read_space(void * coro_local_addr, KVReqCtx * ctx);

    // for update optimize

    BreakDown *breakdown;

    int find_seq[4] = {1, 0, 3, 2};

    uint64_t connect_parity_key_to_64int(int stripeid, int parityid);
    void send_one_sr_list(IbvSrList * sr_list, map<uint64_t, bool> *comp_wrid_map);

    void init_bucket_space(BucketBuffer *bucketbuffer, uint64_t & ptr);
    void init_bucket_space_big_slot(BucketBuffer *bucketbuffer, uint64_t & ptr);

    // RDMA print info
    void print_error(const char * err);
    void print_mes(const char *mes);
    void print_sp(const char *sp);
    void print_args(const char *args_name, double value);

    void print_encoding_mes();
    void print_buffer_mes();
    void print_buffer_all_mes();

    int encoding_check_index;

    uint64_t faa_need_value_WL;
    uint64_t faa_need_value_RL;

    bool test_sync_faa_async();
    bool test_sync_read_async();
    bool test_sync_read_async_double();

    struct timeval recover_st_;
    struct timeval connection_recover_et_;
    struct timeval mm_recover_et_;
    struct timeval local_mr_reg_et_;
    struct timeval ec_init_et_;
    struct timeval middle_et_;
    struct timeval get_wp_rp_middle_et_;
    struct timeval get_wp_rp_et_;
    struct timeval get_unicol_log_et;
    struct timeval kv_ops_recover_et_;

    bool is_recovery;

    MemoryConsumption mem_con;

    int test_num;

    struct timespec *pre_time;
    uint64_t *lat_list;
    void init_time_latency(int num_op);
    int op_id;
    int num_failed_;
    void get_time_latency(bool if_failed);
    void print_time_latency();

    void req_init_sr_list(KVReqCtx *ctx);
    int req_batch_send_and_wait(KVReqCtx *ctx);
    void req_append_sr_list(KVReqCtx *ctx, IbvSrList *sr_list);

    pthread_t start_polling_thread();
    void stop_polling_thread();
};

typedef struct TagClientFiberArgs {
    ClientFR   * client;
    uint32_t ops_st_idx;
    uint32_t ops_num;
    uint32_t coro_id;

    uint32_t num_failed;
    
    // for count time
    struct timeval * st;
    struct timeval * et;

    // for count ops
    boost::fibers::barrier * b;
    volatile bool   * should_stop;
    uint32_t ops_cnt;
    uint32_t thread_id;
    uint64_t tpt;

    NumCount code_num_failed;

    int sleep_ms;

} ClientFRFiberArgs;

typedef struct TagEncodingThread{
    ClientFR *client;
    KVReqCtx * ctx;
}EncodingThread;

void * client_ops_fb_cnt_time(void * arg);
void * client_ops_fb_cnt_ops_cont_fr(void * arg);
void init_num_count(NumCount & num_count);
void add_num_count(NumCount & init_num_count, NumCount & new_num_count);
void print_num_count(NumCount code_num_count);
int get_all_num_failed(NumCount code_num_failed);

uint16_t combined_server_id(vector<uint8_t> & server_list);

void fill_race_slot(RaceHashSlot *slot, uint8_t fp, uint8_t kv_len, uint8_t server_id, uint64_t addr);
uint64_t find_empty_slot_get_offset(uint64_t bucket_addr);
void init_req_comp_wrid_map(KVReqCtx *ctx);

#endif