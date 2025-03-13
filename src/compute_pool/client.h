#ifndef DDCKV_CLIENT_H_
#define DDCKV_CLIENT_H_

#include "client_mm.h"
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
#define DEGREAD_READ_METADATA       1400
#define READ_P0                     1500
#define READ_LOGGING                1600
#define CAS_PL_bucket               1700
#define READ_CACHE_KV               1800
#define WRITE_PARITY_CHUNK          1900
#define WRITE_METADATA_CHUNK        2000
#define CAS_PARITY_BUCKET           2100
#define CAS_METADATA_BUCKET         2200
#define CAS_STRIPE_BUCKET           2300
#define READ_PARITY_CHUNK_BUCKET    2400
#define READ_METADATA_BUCKET        2500
#define INSERT_WRITE_KV_LOG         2600
#define INSERT_WRITE_KV_LOG_ENTRY   2700
#define INSERT_WRITE_FAA_WP_LOG     2800
#define UPDATE_CAS_STRIPE_META_LOG  2900
#define UPDATE_FAA_STRIPE_META_LOG  3000
#define UPDATE_WRITE_KV_SLOT_LOG    3100
#define UPDATE_WRITE_PL_SLOT_LOG    3200
#define DELETE_CAS_STRIPE_META_LOG  3300
#define DELETE_FAA_STRIPE_META_LOG  3400
#define DELETE_WRITE_PL_SLOT_LOG    3500
#define DELETE_WRITE_KV_SLOT_LOG    3600
#define DELETE_WRITE_KV_META_LOG    3700
#define DEGRADE_CAS_STRIPE_META_LOG 3800
#define DEGRADE_FAA_STRIPE_META_LOG 3900
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
#define DEGREAD_KV_META             5000
#define DEGREAD_LOG_END             5100
#define RECOVERY_READ_WP            5200
#define RECOVERY_READ_RP            5300
#define RECOVERY_READ_LOG_ENTRY     5400

#define READ_KV                     10000
#define READ_KV_META_BUCKET         40000
#define WRITE_KV_SLOT        65000

#define MAX_TEST 1000000

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

// add update breakdown bottleneck test
enum update_breakdown{
    UPDATE_ALL,
    UPDATE_READ_BUCKET,
    UPDATE_WRITE_KV,
    UPDATE_READ_KV,
    UPDATE_READ_STRIPE_META,
    UPDATE_CAS_STRIPE_META,
    UPDATE_ENCODING_PARITY,
    UPDATE_WRITE_STRIPE_META_LOG,
    UPDATE_WRITE_KV_LOG,
    UPDATE_WRITE_PL1,
    UPDATE_WRITE_PL2,
    UPDATE_WRITE_PL_LOG1,
    UPDATE_WRITE_PL_LOG2,
    UPDATE_READ_PL_BUCKET,
    UPDATE_CAS_PL_SLOT1,
    UPDATE_CAS_PL_SLOT2,
    UPDATE_CAS_KV_SLOT,
    UPDATE_FAA_STRIPE_META,
    UPDATE_WRITE_END_LOG,
};

#define NUM_UPDATE_OPS (UPDATE_WRITE_END_LOG - UPDATE_ALL + 1)

struct OneTimer{
    timespec start_time;
    timespec end_time;
};

class ReqTimer{
    public:
        int op_type;
        int num_ops;
        OneTimer *timer_list;
    public:
        ReqTimer(int op_type);
        ~ReqTimer();
        void start_timer(int bd);
        void end_timer(int bd);
};

class ReqLatency{
    public:
        int num_req;
        int op_type;
        int num_ops;
        int pre_req;
        double **latency_list;
    public:
        ReqLatency(int num_req, int op_type);
        ~ReqLatency();
        void add_req_latency(ReqTimer *req_timer);
        void print_all_latency();
};

double get_timer_spec_us(OneTimer *timer);
int get_num_ops_from_type(int op_type);

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

struct CasKvMessage{

    uint8_t server_id;
    uint64_t remote_slot_addr;
    uint64_t new_value;
    uint64_t local_addr;

};

struct CasRecoveryEle{

    int recovery_times;
    int recovery_when;
    CasKvMessage cas_kv_mes;

};

class CasRecovery{

    public:

        int num_coro;
        queue <CasRecoveryEle> *cas_rec_ele_queue;

    public:

        CasRecovery(int num_coro);
        ~CasRecovery();

        void insert(int & coro_id, CasRecoveryEle *ele);

        CasRecoveryEle * check_coro(uint32_t & coro_id);

        void remove(int coro_id);

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
    KVInfo *kv_meta_info;
    KVHashInfo *kv_meta_hash_info;  
    KVTableAddrInfo *kv_meta_addr_info;

    KVInfo *stripe_meta_kv_info;
    KVHashInfo *stripe_meta_hash_info;
    KVTableAddrInfo *stripe_meta_addr_info;
    BucketBuffer stripe_meta_bucket_info;

    int stripe_id;
    KvEcMetaCtx kv_meta_ctx;
    int find_kv_meta_num;

    std::vector<IbvSrList *> sr_list_batch;
    std::vector<uint32_t>    sr_list_num_batch;
    std::map<uint8_t, std::vector<IbvSrList *> > server_id_sr_list_map_;

    BucketSlotIdx stripe_meta_idx;
    StripeMetaCtx stripe_meta_ctx;

    KVInfo *PL_info;
    KVHashInfo *PL_hash_info;
    KVTableAddrInfo *PL_addr_info;
    BucketBuffer *PL_bucket_buffer;
    u8 *parity_logging_data; 

    int parity_logging_size;

    ClientMMAllocCtx mm_alloc_parity_logging_ctx[MAX_M];

    int PL_match_idx;

    // optimize sr list send workflow
    map<uint64_t, bool> *comp_wrid_map;

    int *parity_id;

    bool is_finished;
    union {
        void * value_addr;    // for search return value
        int    ret_code;
    } ret_val;

    int coro_id;

    // add log
    bool is_cas;

    uint64_t stripe_meta_log_buf_addr;
    uint64_t stripe_meta_end_log_buf_addr;
    uint64_t kv_slot_log_buf_addr;
    uint64_t *PL_slot_log_buf_addr;

    uint64_t stripe_meta_log_return_addr;
    uint64_t kv_slot_log_return_addr;
    uint64_t *PL_slot_log_return_addr;

    CasRecoveryEle *cas_rec_ele;

}UpdateCtx;

typedef struct TagDeleteCtx {

    CasRecoveryEle *cas_rec_ele;

    KVInfo *kv_meta_info;
    KVHashInfo *kv_meta_hash_info;  
    KVTableAddrInfo *kv_meta_addr_info;
    BucketBuffer *kv_meta_bucket_buffer;
    KvEcMetaCtx *kv_meta_ctx;

    KVInfo *stripe_meta_info;
    KVHashInfo *stripe_meta_hash_info;
    KVTableAddrInfo *stripe_meta_addr_info;
    BucketBuffer *stripe_meta_bucket_info;
    StripeMetaCtx *stripe_meta_ctx;
    BucketSlotIdx *stripe_meta_idx;

    KVInfo *PL_info;
    KVHashInfo *PL_hash_info;
    KVTableAddrInfo *PL_addr_info;
    BucketBuffer *PL_bucket_buffer;

    u8 *parity_logging_data; 
    int parity_logging_size;

    ClientMMAllocCtx *mm_alloc_parity_logging_ctx;

    int PL_match_idx;

    map<uint64_t, bool> *comp_wrid_map;

    uint64_t old_kv_meta_value;

    int *parity_id;
    int coro_id;
    bool is_finished;
    int stripe_id;

    union {
        void * value_addr;    // for search return value
        int    ret_code;
    } ret_val;

    // add log
    bool is_cas;

    uint64_t stripe_meta_log_buf_addr;
    uint64_t kv_slot_log_buf_addr;
    uint64_t stripe_meta_end_log_buf_addr;
    uint64_t *kv_meta_log_buf_addr;
    uint64_t *PL_slot_log_buf_addr;

    uint64_t stripe_meta_log_return_addr;
    uint64_t kv_slot_log_return_addr;
    uint64_t *kv_meta_log_return_addr;
    uint64_t *PL_slot_log_return_addr; 

    int num_idx_rep;
    
} DeleteCtx;

typedef struct TagDegradeReadCtx {
    KVInfo * kv_meta_info;
    KVHashInfo * kv_meta_hash_info;
    KVTableAddrInfo * kv_meta_addr_info;
    BucketBuffer * kv_meta_bucket_buffer;

    KVInfo * stripe_meta_info;
    KVHashInfo * stripe_meta_hash_info;
    KVTableAddrInfo * stripe_meta_addr_info;
    BucketBuffer * stripe_meta_bucket_buffer;
    
    KVInfo * ec_meta_info;
    KVHashInfo * metadata_hash_info;
    KVTableAddrInfo * metadata_addr_info;
    BucketBuffer * metadata_bucket_buffer;

    KVInfo * P0_info;
    KVHashInfo * P0_hash_info;
    KVTableAddrInfo * P0_addr_info;
    BucketBuffer * P0_bucket_buffer;

    KVInfo * logging_info;
    KVHashInfo * PL_hash_info;
    KVTableAddrInfo * PL_addr_info;
    BucketBuffer * PL_bucket_buffer;

    KVInfo * kv_info;
    KVHashInfo * kv_hash_info;
    KVTableAddrInfo * kv_addr_info;
    BucketBuffer * kv_bucket_buffer;

    vector<IbvSrList *> sr_list_batch;
    vector<uint32_t>    sr_list_num_batch;

    // get from kv meta
    KvEcMetaCtx *kv_meta_ctx;

    int stripe_id;

    // get (k - 1) metadata
    vector<KVRWAddr> *kv_read_metadata_addr_list;
    vector<pair<int32_t, int32_t>> *kv_metadata_idx_list;

    // cas stripe meta
    // vector<pair<int32_t, int32_t>> *kv_stripe_meta_idx_list;
    BucketSlotIdx *stripe_meta_idx;
    StripeMetaCtx *stripe_meta_ctx;

    int num_block;
    uint64_t pre_kv_meta;

    // degrade need kv message
    NeedKv *need_kv;
    int num_need_kv;

    int repair_kv_num;

    // get P0
    vector<KVRWAddr> kv_read_P0_addr_list;
    vector<pair<int32_t, int32_t>> kv_P0_idx_list;

    // get logging
    vector<KVRWAddr> kv_read_logging_addr_list;
    // vector<pair<int32_t, int32_t>> kv_logging_idx_list;

    // get kv
    vector<KVRWAddr> kv_read_kv_addr_list;
    // vector<pair<int32_t, int32_t>> kv_kv_idx_list;

    // for workflow overlap
    map<uint64_t, bool> *comp_wrid_map;

    // for decoding
    u8 **encoding_data;

    // for local metadata buf
    void *ec_meta_buf_;
    ibv_mr *ec_meta_buf_mr_;

    int coro_id;
    
    // for log
    bool is_cas;

    uint64_t stripe_meta_log_buf_addr;
    uint64_t stripe_meta_log_return_addr;

    uint64_t stripe_meta_end_log_buf_addr;

    bool is_finished;
    union {
        void * value_addr;    // for search return value
        int    ret_code;
    } ret_val;

    int *parity_id;

}DegradeReadCtx;

typedef struct TagEncodingCtx {

    RsCoding *rs_coding;
    u8 **parity_chunk;
    u8 **metadata_chunk;

    void *buf_;
    ibv_mr *buf_mr_;

    void *parity_buf;
    void *metadata_buf;
    void *bucket_buf;
    void *map_buf;

    KVInfo *parity_info;
    KVHashInfo *parity_hash_info;
    KVTableAddrInfo *parity_addr_info;
    BucketBuffer *parity_bucket_buffer;

    ClientMMAllocCtx *mm_alloc_parity_ctx;

    KVInfo *metadata_info;
    KVHashInfo *metadata_hash_info;
    KVTableAddrInfo *metadata_addr_info;
    BucketBuffer *metadata_bucket_buffer;
    
    ClientMMAllocCtx *mm_alloc_metadata_ctx;

    KVHashInfo *stripe_hash_info;
    KVTableAddrInfo *stripe_addr_info;
    BucketBuffer *stripe_bucket_buffer;

    StripeMetaCtx stripe_meta_ctx;

    KVInfo **kv_meta_info;
    KVHashInfo **kv_meta_hash_info;
    KVTableAddrInfo **kv_meta_addr_info;
    BucketBuffer ***kv_meta_bucket_buffer;

    KvEcMetaCtx **kv_meta_ctx;

    uint64_t *kv_meta_buckets_buffer_addr;

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

    // add breakdown bottleneck
    ReqTimer *req_timer;

    map<int, int> wr_id_map_op_id;

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

// end

class Client {
private:

    UDPNetworkManager * nm_;
    ClientMM * mm_;

    uint32_t client_id;

    uint64_t pr_log_tail_;

    uint64_t remote_global_meta_addr_;
    uint64_t remote_sync_addr;

    uint64_t remote_PL_map_addr;
 
    uint64_t remote_root_addr_;

    // remote hash table root addr
    uint64_t remote_kv_ec_meta_root_addr_;
    uint64_t remote_stripe_meta_root_addr_;
    uint64_t remote_PL_root_addr_;
    uint64_t remote_metadata_root_addr_;

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

    inline int nm_check_completion_bd(std::map<uint64_t, bool> & wait_wrid_wc_map, map<int, int> & wr_id_map_op_id, ReqTimer *req_timer){
        std::map<uint64_t, bool>::iterator it;
        for (it = wait_wrid_wc_map.begin(); it != wait_wrid_wc_map.end(); it ++) {
            uint64_t wrid = it->first;
            tbb::concurrent_hash_map<uint64_t, struct ibv_wc *>::const_accessor acc;
            if (nm_->wrid_wc_map_.find(acc, wrid)) {
                wait_wrid_wc_map[wrid] = true;
                nm_->wrid_wc_map_.erase(acc);
                req_timer->end_timer(wr_id_map_op_id[wrid]);
            }
        }
        return 0;
    }

    inline int poll_completion_bd(std::map<uint64_t, bool> & wait_wrid_wc_map, map<int, int> & wr_id_map_op_id, ReqTimer *req_timer){
        int ret = 0;
        while (nm_->is_all_complete(wait_wrid_wc_map) == false) {
            boost::this_fiber::sleep_for(std::chrono::microseconds(10));
            ret = nm_check_completion_bd(wait_wrid_wc_map, wr_id_map_op_id, req_timer);
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
    void init_count_server();
    void print_count_server();
    CasRecovery* cas_rec;
    int *count_used_kv_slot;
    void init_count_used_slot();
    void print_count_used_slot();
    int num_count[6];
    Client(struct GlobalConfig * conf);
    ~Client();
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
    void encoding_check_async();
    void encoding_check_async_make_trace();
    void encoding_leave();
    void encoding_leave_make_trace();

    void encoding_prepare_sync();
    void encoding_prepare_async();
    void encoding_make_parity_metadata();
    void encoding_write_and_read_buckets_sync();
    void encoding_write_and_read_buckets_async();
    void encoding_parity();
    void encoding_metadata();
    void encoding_stripe();
    void encoding_kv_meta();
    void encoding_prepare_parity();
    void encoding_prepare_metadata();
    void encoding_prepare_stripe();
    void prepare_stripe_meta(int stripe_id, KVHashInfo *hash_info, KVTableAddrInfo *addr_info);
    void encoding_prepare_kv_meta();
    void encoding_cas_sync();
    void encoding_cas_async();
    void encoding_cas_parity();
    void encoding_cas_metadata();
    void encoding_cas_stripe();
    void encoding_write_kv_meta();
    void encoding_cas_check();

    // add encoding change log pointer
    void encoding_faa_log_sync();
    void encoding_faa_log_async();
    void encoding_init_sr_list();
    void encoding_finish();
    void encoding_finish_trace();
    void encoding_and_make_stripe_sync();
    void encoding_and_make_stripe_sync_trace();
    void encoding_and_make_stripe_async();
    void encoding_and_make_stripe_async_trace();

    IbvSrList *gen_one_sr_list(uint64_t local_addr, uint32_t local_len, uint32_t lkey, 
        uint32_t coro_id, uint8_t server_id, uint32_t type, uint32_t wr_id_offset, 
        ibv_wr_opcode opcode, uint64_t remote_addr, int send_flags, ibv_send_wr *next);

    IbvSrList *gen_one_sr_list_batch(uint64_t local_addr, uint32_t local_len, uint32_t lkey, 
        uint32_t coro_id, uint8_t server_id, uint32_t type, uint32_t wr_id_offset, 
        ibv_wr_opcode opcode, uint64_t remote_addr, int send_flags, ibv_send_wr *next);

    IbvSrList *gen_one_sr_list_encoding(uint64_t local_addr, uint32_t local_len, uint32_t lkey, 
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
    int load_kv_req(int num_op, const char * op);
    int load_kv_req_file_twitter(const char * fname, uint32_t st_idx, int32_t num_ops);
    int load_kv_req_file_mncrashed(const char * fname, uint32_t st_idx, int32_t num_ops);
    int load_kv_req_from_file_ycsb(const char * fname, uint32_t st_idx, int32_t num_ops);
    void load_kv_req_init_update(KVReqCtx *ctx);
    void load_kv_req_init_delete(KVReqCtx *ctx);
    void load_kv_req_init_degrade_read(KVReqCtx * ctx);
    void init_kv_req_ctx(KVReqCtx * ctx, char * operation);
    void init_kv_req_ctx_plus(KVReqCtx * ctx, char * operation, uint64_t & input_buf_ptr, uint64_t & used_len);
    void init_kv_req_ctx_plus_ycsb(KVReqCtx * ctx, char * operation, uint64_t & input_buf_ptr, uint64_t & used_len);

    // kv_op
    int kv_insert_sync(KVReqCtx * ctx);
    int kv_insert_sync_batch(KVReqCtx * ctx);
    int kv_insert_crash_sync(KVReqCtx * ctx);
    int kv_insert_only_buffer(KVReqCtx * ctx);
    int kv_insert_posibility_crash_sync(KVReqCtx * ctx);
    void * kv_search_sync(KVReqCtx * ctx);
    void * kv_search_sync_batch(KVReqCtx * ctx);
    int kv_update_sync(KVReqCtx * ctx);
    int kv_delete_sync(KVReqCtx * ctx);
    int kv_insert(KVReqCtx * ctx);
    void * kv_search(KVReqCtx * ctx);
    int kv_update(KVReqCtx * ctx);
    int kv_update_cas_rec(KVReqCtx *ctx, CasRecoveryEle *cas_rec_ele);
    void kv_update_cas_rec_RTT1(KVReqCtx *ctx, CasRecoveryEle *cas_rec_ele);
    void kv_update_cas_rec_RTT2(KVReqCtx *ctx, CasRecoveryEle *cas_rec_ele);
    void kv_update_cas_rec_RTT2_check(KVReqCtx *ctx, CasRecoveryEle *cas_rec_ele, uint64_t old_value);
    void * kv_degread_sync(KVReqCtx * ctx);
    void * kv_degrade_read(KVReqCtx * ctx);

    // for breakdown
    void *kv_degread_bd(KVReqCtx *ctx);
    void kv_degread_RTT1_bd(KVReqCtx *ctx);
    void kv_degread_read_kv_index_bd(KVReqCtx *ctx);
    void kv_degread_RTT2_bd(KVReqCtx *ctx);
    void kv_degread_read_metadata_index_bd(KVReqCtx *ctx);
    void kv_degread_read_P0_index_bd(KVReqCtx *ctx);
    void kv_degread_read_stripe_meta_bd(KVReqCtx *ctx);
    void kv_degread_RTT3_bd(KVReqCtx *ctx);
    void kv_degread_cas_stripe_meta_bd(KVReqCtx *ctx);
    void kv_degread_read_metadata_bd(KVReqCtx *ctx);
    void kv_degread_read_PL_index_bd(KVReqCtx *ctx);
    void kv_degread_write_stripe_log_bd(KVReqCtx *ctx);
    void kv_degread_RTT4_bd(KVReqCtx *ctx);
    void kv_degread_read_P0_bd(KVReqCtx *ctx);
    void kv_degread_read_PL_bd(KVReqCtx *ctx);
    void kv_degread_read_related_kv_index_bd(KVReqCtx *ctx);
    void kv_degread_RTT5_bd(KVReqCtx *ctx);
    void kv_degread_read_related_kv_bd(KVReqCtx *ctx);
    void kv_degread_RTT6_bd(KVReqCtx *ctx);
    void kv_degread_faa_stripe_meta_bd(KVReqCtx *ctx);
    void kv_degread_decoding_bd(KVReqCtx *ctx);
    void kv_degread_write_end_log_bd(KVReqCtx *ctx);

    // delete
    int kv_delete(KVReqCtx * ctx);
    void kv_delete_RTT1_async(KVReqCtx *ctx);
    void kv_delete_RTT1_read_bucket(KVReqCtx *ctx);
    void kv_delete_send(DeleteCtx *dctx, IbvSrList *sr_list, uint32_t sr_list_num);
    void kv_delete_post_and_wait(DeleteCtx *dctx);
    void kv_delete_RTT2_async(KVReqCtx *ctx);
    void delete_analysis_kv_meta(KVReqCtx *ctx);
    void kv_delete_prepare_stripe_meta(DeleteCtx *dctx);
    void kv_delete_get_stripe_meta(KVReqCtx *ctx);
    void kv_delete_RTT3_async(KVReqCtx *ctx);
    void kv_delete_RTT3_write_kv_meta_log(KVReqCtx *ctx);
    void kv_delete_RTT4_async(KVReqCtx *ctx);
    void kv_delete_RTT4_kv(KVReqCtx * ctx);
    void kv_delete_RTT4_kv_meta(KVReqCtx *ctx);
    void kv_delete_check_write_PL_and_write_kv(KVReqCtx * ctx);
    void kv_delete_RTT5_async(KVReqCtx *ctx);
    void kv_delete_RTT5_log(DeleteCtx *dctx);

    void kv_delete_RTT1_sync(KVReqCtx *ctx);
    void kv_delete_RTT2_sync(KVReqCtx *ctx);
    void kv_delete_RTT3_sync(KVReqCtx *ctx);
    void kv_delete_RTT4_sync(KVReqCtx *ctx);
    void kv_delete_RTT5_sync(KVReqCtx *ctx);
    void kv_degrade_read_RTT_2(KVReqCtx *ctx);

    pthread_t start_polling_thread();
    void stop_polling_thread();

// private methods
private:
    bool init_is_finished();
    int  sync_init_finish();
    int  init_log();
    int  connect_ib_qps();

    void init_all_hash_table_mem();
    void read_all_hash_table();
    void init_other_client_PL_map();
    void write_all_hash_table();
    int init_all_hash_index();
    int init_all_hash_subtable();

    void mm_alloc_to_subtable_entry(RaceHashSubtableEntry & entry, ClientMMAllocSubtableCtx & mmctx);

    // kv_req relation
    void prepare_request(KVReqCtx * ctx);
    void get_kv_addr_info(KVHashInfo * hash_info, __OUT KVTableAddrInfo * addr_info);
    void get_kv_hash_info(KVInfo * kv_info, __OUT KVHashInfo * hash_info);

    // update req relation
    void kv_update_init_sr_list(KVReqCtx * ctx);
    void kv_update_RTT1_read_bucket_and_write_kv(KVReqCtx * ctx);
    void kv_update_read_kv(KVReqCtx * ctx);
    void kv_update_get_stripe_meta(KVReqCtx * ctx);
    void kv_update_analysis_kv_meta(KVReqCtx * ctx);
    int find_kv_meta_slot(void * bucketbuffer, KvEcMetaCtx *kv_meta_ctx, int fp);
    bool find_stripe_meta_slot_struct(BucketBuffer * bucketbuffer, StripeMetaCtx *stripe_meta_ctx, int stripe_id, BucketSlotIdx *idx_list);
    void get_bucket_info(BucketBuffer * bucketbuffer);
    void get_bucket_info_big(BucketBuffer * bucketbuffer);
    void kv_update_prepare_stripe_meta(UpdateCtx * uctx);
    
    /*
        update RTT3: 
        cas stripe meta and write log entry
        write kv log and entry
        read one PL bucket and write m PL and m PL log
    */ 
    void kv_update_RTT3_async(KVReqCtx * ctx);
    void kv_update_RTT3_check(KVReqCtx *ctx);
    void kv_degread_RTT3_check(KVReqCtx *ctx);

    // turn faa to (faa and cas)
    void kv_update_cas_stripe_meta_first(KVReqCtx *ctx);
    void kv_update_cas_stripe_meta_second(KVReqCtx *ctx);
    void kv_update_RTT5_log(KVReqCtx *ctx);
    void kv_update_cas_stripe_meta(KVReqCtx *ctx, bool if_add);
    void kv_update_make_cas_stripe_meta(UpdateCtx *uctx);
    void kv_update_analysis_stripe_meta(UpdateCtx *uctx);
    
    void kv_update_prepare_PL(KVReqCtx * ctx);
    void kv_update_prepare_PL_info(KVReqCtx * ctx);
    void kv_update_get_PL_hash_info(UpdateCtx * uctx);
    void kv_update_get_PL_addr_info(UpdateCtx * uctx);
    void kv_update_prepare_read_PL_bucket_sr_list(KVReqCtx * ctx);
    void kv_update_prepare_write_PL_sr_list(KVReqCtx * ctx);
    void kv_update_RTT4_kv(KVReqCtx * ctx);
    void kv_update_RTT4_PL(KVReqCtx * ctx);
    void kv_update_check_write_PL_and_write_kv(KVReqCtx * ctx);
    // update RTT5: faa stripe meta and write log
    void kv_update_RTT5_async(KVReqCtx * ctx);
    IbvSrList * gen_faa_sr_lists(uint32_t coro_id, const std::vector<KVCASAddr> & cas_addr_list,
        __OUT uint32_t * num_sr_lists, uint64_t faa_value);

    // degrade read   workflow
    IbvSrList * gen_bucket_sr_lists(int coro_id, KVTableAddrInfo *tbl_addr_info, 
        BucketBuffer *bucket_buffer, __OUT uint32_t * num_sr_lists, int wr_add);
    IbvSrList * gen_bucket_sr_lists_big(int coro_id, KVTableAddrInfo *tbl_addr_info, 
        BucketBuffer *bucket_buffer, __OUT uint32_t * num_sr_lists, int wr_add);
    IbvSrList * gen_bucket_sr_lists_rep(int coro_id, KVTableAddrInfo *tbl_addr_info, 
        BucketBuffer *bucket_buffer, int wr_add, int rep_id);
    IbvSrList * gen_bucket_sr_lists_parity(int coro_id, KVTableAddrInfo *tbl_addr_info, 
        BucketBuffer *bucket_buffer, __OUT uint32_t * num_sr_lists, int wr_add, int lkey);
    IbvSrList * gen_bucket_sr_lists_metadata(int coro_id, KVTableAddrInfo *tbl_addr_info, 
        BucketBuffer *bucket_buffer, __OUT uint32_t * num_sr_lists, int wr_add, int lkey);
    void kv_degread_init_sr_list(KVReqCtx * ctx);
    int kv_degrade_send_sr_list(KVReqCtx * ctx);

    // degrade tpt
    void kv_degread_RTT1_async(KVReqCtx * ctx);
    void kv_degread_RTT2_async(KVReqCtx * ctx);
    void kv_degread_RTT3_async(KVReqCtx * ctx);
    void kv_degread_RTT4_async(KVReqCtx * ctx);
    void kv_degread_RTT5_async(KVReqCtx * ctx);
    void kv_degread_RTT6_async(KVReqCtx * ctx);

    // degrade first  workflow: get kv meta to get stripe id, obj offset, parity 0 ring id
    void kv_degread_RTT1_sync(KVReqCtx * ctx);
    void kv_degread_read_kv_meta(KVReqCtx * ctx);
    
    // degrade second workflow: get ec meta bucket;get stripe meta to get wl,rl;get P0 and its loggings bucket
    void hash_compute_fp(KVHashInfo * hash_info);
    // hash_value -> server id(m + 1);subtable start addr(m + 1);subtable bucket offset(bucket id)
    void get_server_subtable_kv(KVHashInfo * hash_info, KVTableAddrInfo *addr_info);
    void get_server_subtable_parity(KVHashInfo * hash_info, KVTableAddrInfo *addr_info, int bufferid);
    void get_server_subtable_stripe_meta(KVHashInfo * hash_info, KVTableAddrInfo *addr_info);
    void get_server_subtable_metadata(uint32_t stripe_id, KVHashInfo *all_hash_info, KVTableAddrInfo *addr_info);
    void get_metadata_subtable_addr(uint32_t stripe_id, KVHashInfo *all_hash_info, KVTableAddrInfo *addr_info);
    void get_two_combined_bucket_index(KVHashInfo * hash_info, KVTableAddrInfo *addr_info);
    void get_bucket_index(KVHashInfo * hash_info, KVTableAddrInfo *addr_info);
    void get_rep_server_subtable_addr(KVHashInfo * hash_info, KVTableAddrInfo *addr_info, uint8_t type);
    void get_PL_server_subtable_addr(KVHashInfo * hash_info, KVTableAddrInfo *addr_info, int *pid);
    void init_bucket_space_havekv(BucketBuffer * bucket_buffer);

    void kv_degread_RTT2_sync(KVReqCtx * ctx);
    void kv_degrade_read_analysis_kv_meta(KVReqCtx * ctx);
    void kv_degread_prepare_metadata_bucket(KVReqCtx * ctx);
    void kv_degread_metadata_hash_info(KVReqCtx * ctx);
    void kv_degrade_metadata_addr_info(KVReqCtx * ctx);
    void kv_degread_metadata_bucket(KVReqCtx * ctx);
    void kv_degrade_prepare_get_stripe_meta(KVReqCtx * ctx);
    void kv_degread_stripe_meta(KVReqCtx * ctx);
    void kv_degread_prepare_get_P0_bucket(KVReqCtx * ctx);
    void kv_degrade_prepare_get_P0_bucket_hash_info(KVReqCtx * ctx);
    void kv_degrade_prepare_get_P0_bucket_addr_info(KVReqCtx * ctx);
    void kv_degread_get_P0_bucket(KVReqCtx * ctx);
    void kv_degread_prepare_get_PL_bucket(KVReqCtx * ctx);
    void kv_degread_prepare_PL_bucket_hash_info(KVReqCtx * ctx);
    void kv_degread_prepare_PL_bucket_addr_info(KVReqCtx * ctx);
    void kv_degread_get_PL_bucket(KVReqCtx * ctx);

    // degrade third  workflow: get ec meta block;faa stripe meta's wl first
    void kv_degread_RTT3_sync(KVReqCtx * ctx);
    int race_find_bucket_no_idx(RaceHashSlot *slot[4], KVHashInfo *hash_info, 
        uint64_t *local_buf_addr, uint32_t lkey, vector<KVRWAddr> & read_addr_list);
    void fill_cas_slot_through_bucket_buffer_value(
        KVCASAddr * cur_cas_addr, int server_id, uint32_t lkey,
        uint64_t remote_slot_addr, uint64_t return_local_slot_addr,
        uint64_t old_value, uint64_t new_value);
    void kv_degread_analysis_metadata_bucket(KVReqCtx * ctx);
    void kv_degread_analysis_stripe_meta(DegradeReadCtx * drctx);
    void kv_degread_get_metadata(KVReqCtx * ctx);
    void kv_degread_cas_stripe_meta_first(DegradeReadCtx *drctx);
    void kv_degread_cas_stripe_meta_second(DegradeReadCtx *drctx);
    void kv_degread_make_cas_stripe_meta(DegradeReadCtx *drctx);
    void kv_degread_cas_stripe_meta(DegradeReadCtx *drctx, bool if_add);
    void kv_degread_last_RTT_log(DegradeReadCtx *drctx);

    // degrade fourth workflow: get related kvs bucket;get old P0 and its loggings 
    void kv_degread_RTT4_sync(KVReqCtx * ctx);
    void kv_degread_analysis_metadata(KVReqCtx * ctx);
    void kv_degread_prepare_get_kv_bucket(KVReqCtx * ctx);
    void kv_degread_prepare_get_kv_bucket_hash_info(KVReqCtx * ctx);
    void kv_degread_prepare_get_kv_bucket_addr_info(KVReqCtx * ctx);
    void kv_degread_get_kv_bucket(KVReqCtx * ctx);
    void kv_degread_analysis_P0_bucket(KVReqCtx * ctx);
    void kv_degread_get_P0(KVReqCtx * ctx);
    int kv_degrade_read_analysis_logging_bucket(KVReqCtx * ctx);
    void kv_degread_get_PL(KVReqCtx * ctx);

    // degrade fifth  workflow: get related kvs;decoding
    void kv_degread_RTT5_sync(KVReqCtx * ctx);
    void kv_degread_read_analysis_kv_bucket(KVReqCtx * ctx);
    void kv_degread_prepare_get_kv(KVReqCtx * ctx);
    
    // degrade sixth  workflow: faa stripe meta's wl second
    void kv_degread_RTT6_sync(KVReqCtx * ctx);
    void kv_degread_read_compute_kv(KVReqCtx * ctx);
    void kv_degread_read_merge_logging(KVReqCtx * ctx);
    void kv_degread_read_decoding(KVReqCtx * ctx);

    bool find_P0_slot(void *bucket, uint8_t fp, uint64_t *local_addr, vector<KVRWAddr> & read_addr_list, 
        vector<std::pair<int32_t, int32_t>> & idx_list);

    // kv delete
    void kv_delete_init_sr_list(DeleteCtx *delete_ctx);

    // delete RTT3:cas stripe meta read PL buckets and write PL
    void kv_delete_cas_stripe_meta_first(DeleteCtx *dctx);
    void kv_delete_analysis_stripe_meta(DeleteCtx *dctx);
    void kv_delete_cas_stripe_meta(DeleteCtx *dctx, bool if_add);
    void kv_delete_make_cas_stripe_meta(DeleteCtx *dctx);
    void kv_delete_prepare_PL(KVReqCtx *ctx);
    void kv_delete_compute_PL(KVReqCtx *ctx);
    void kv_delete_get_PL_hash_info(DeleteCtx *dctx);
    void kv_delete_get_PL_addr_info(DeleteCtx *dctx);
    void kv_delete_write_kv_log(KVReqCtx *ctx);
    void kv_delete_prepare_write_PL_sr_list(KVReqCtx * ctx);
    void kv_delete_prepare_read_PL_bucket_sr_list(KVReqCtx * ctx);
    void kv_delete_RTT4_PL(KVReqCtx * ctx);

    // delete RTT4:write kv buckets and write PL buckets
    bool find_empty_slot_from_bucket_buffer(BucketBuffer *bucketbuffer, TagKVTableAddrInfo *addr_info);
    bool find_empty_slot_from_bucket_buffer_big(BucketBuffer *bucketbuffer, TagKVTableAddrInfo *addr_info);

    // delete RTT5:faa stripe meta
    void kv_delete_cas_stripe_meta_second(DeleteCtx *dlctx);

    // kv relation
    IbvSrList * gen_write_kv_sr_lists(uint32_t coro_id, KVInfo * a_kv_info,    
        ClientMMAllocCtx * r_mm_info, __OUT uint32_t * num_sr_lists);
    IbvSrList * gen_write_kv_sr_lists(uint32_t coro_id, KVInfo * a_kv_info,    
        ClientMMAllocCtx * r_mm_info, __OUT uint32_t * num_sr_lists, int id);
    IbvSrList * gen_write_kv_sr_lists_encoding(uint32_t coro_id, KVInfo * a_kv_info,    
        ClientMMAllocCtx * r_mm_info, __OUT uint32_t * num_sr_lists, int id);
    IbvSrList * gen_cas_sr_lists(uint32_t coro_id, const std::vector<KVCASAddr> & cas_addr_list,
        __OUT uint32_t * num_sr_lists);
    IbvSrList * gen_cas_sr_lists_add_id(uint32_t coro_id, const std::vector<KVCASAddr> & cas_addr_list,
        __OUT uint32_t * num_sr_lists, uint64_t add_id);
    IbvSrList * gen_cas_sr_lists_encoding(uint32_t coro_id, const std::vector<KVCASAddr> & cas_addr_list,
        __OUT uint32_t * num_sr_lists, uint64_t add_id);
    IbvSrList * gen_read_kv_sr_lists(uint32_t coro_id, const std::vector<KVRWAddr> & r_addr_list, 
        __OUT uint32_t * num_sr_lists);
    IbvSrList * gen_read_kv_sr_lists_wr_add(uint32_t coro_id, 
        const std::vector<KVRWAddr> & r_addr_list, __OUT uint32_t * num_sr_lists, int wr_add);
    IbvSrList * gen_read_value_sr_lists(uint32_t coro_id, int wr_add, KVRWAddr r_addr_list);

    void kv_insert_read_buckets_and_write_kv_sync(KVReqCtx * ctx);
    void kv_insert_read_buckets_and_write_kv(KVReqCtx * ctx);
    void kv_insert_write_buckets_sync(KVReqCtx * ctx);
    void kv_insert_write_buckets_sync_batch(KVReqCtx * ctx);
    void kv_insert_write_buckets_sync_crash(KVReqCtx * ctx);
    void kv_insert_write_buckets(KVReqCtx * ctx);
    void kv_search_read_buckets_sync(KVReqCtx * ctx);
    void kv_search_read_buckets_sync_batch(KVReqCtx * ctx);
    void kv_search_read_buckets(KVReqCtx * ctx);
    void kv_search_read_kv_sync(KVReqCtx * ctx);
    void kv_search_read_kv_sync_batch(KVReqCtx * ctx);
    void kv_search_read_kv(KVReqCtx * ctx);
    void kv_search_check_kv(KVReqCtx * ctx);
    // update RTT1: read KvEcMetaBucket and write new kv
    void kv_update_RTT1_async(KVReqCtx * ctx);
    // update RTT2: read stripe meta Bucket and read kv chunk
    void kv_update_RTT2_async(KVReqCtx * ctx);
    // update RTT4: cas kv slot and m PL slot
    void kv_update_RTT4_async(KVReqCtx * ctx);
    void get_local_bucket_info_kv(KVReqCtx * ctx);
    void find_empty_kv_slot(KVReqCtx * ctx);
    void find_empty_kv_slot_crash(KVReqCtx * ctx);
    void find_kv_slot(KVReqCtx * ctx);
    bool find_kv_slot_in_buckets(KvEcMetaBucket *bucket, KVRWAddr *rw, 
        pair<int32_t, int32_t> *pair, uint8_t fp, uint64_t local_addr, int op_type);
    int32_t find_match_kv_idx(KVReqCtx * ctx);

    void fill_slot(ClientMMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info,
        __OUT RaceHashSlot * local_slot);

    // add RTT3 sync
    void kv_update_RTT1_sync(KVReqCtx * ctx);
    void kv_update_RTT2_sync(KVReqCtx * ctx);
    void kv_update_RTT3_sync(KVReqCtx * ctx);
    void kv_update_RTT4_sync(KVReqCtx * ctx);
    void kv_update_RTT5_sync(KVReqCtx * ctx);

    /*
        add log
    */

    // insert and encoding log
    uint64_t get_log_entry_addr(uint8_t server_id);
    void log_entry_rp_range(uint8_t server_id);
    IbvSrList *gen_insert_write_kv_log(KVReqCtx * ctx);
    IbvSrList *gen_insert_write_kv_log_entry(KVReqCtx * ctx);
    IbvSrList *gen_faa_log(int coro_id, uint8_t server_id, bool if_wp, 
        uint64_t faa_value, uint64_t return_addr, int lkey, int wr_id_off);
    IbvSrList *gen_faa_log_encoding(int coro_id, uint8_t server_id, bool if_wp, 
        uint64_t faa_value, uint64_t return_addr, int lkey, int wr_id_off);

    // update log
    void kv_update_write_stripe_meta_log(KVReqCtx *ctx);
    void kv_update_write_kv_log(KVReqCtx *ctx);

    // delete log
    void kv_delete_RTT3_check(KVReqCtx *ctx);
    void kv_delete_write_stripe_meta_log(DeleteCtx *dctx);

    // degrade read log
    void kv_degread_write_stripe_meta_log(DegradeReadCtx *drctx);

    IbvSrList *gen_sr_list(uint64_t local_addr, uint32_t local_len, uint32_t lkey, 
        uint32_t coro_id, uint8_t server_id, uint32_t type, uint32_t wr_id_offset, 
        ibv_wr_opcode opcode, uint64_t remote_addr);

// for testing
public:

    // EC message
    int k;
    int m;

    void test_read_latency();
    void test_write_cas_read();
    void test_write_faa_read();

    bool test_read(int size);
    bool test_write(uint64_t local_msg, int size, uint64_t offset);
    bool test_cas(uint64_t swap_value, uint64_t cmp_value);
    bool test_faa(uint64_t add_value, uint64_t offset);

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
    uint64_t connect_metadata_key_to_64int(int stripeid, int off);

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

    void kv_update_send(UpdateCtx *uctx, IbvSrList *sr_list, uint32_t sr_list_num);
    void kv_update_post_and_wait(KVReqCtx *ctx);

    // for recovery

    void get_recover_time(std::vector<struct timeval> & recover_time);

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

    void recovery_clear_sr_list(RecoveryCtx *rcctx);

    void recovery();

    void recovery_init_rcctx(RecoveryCtx *rcctx);
    void recovery_read_wp_rp(RecoveryCtx *rcctx);
    void recovery_read_all_log_entry(RecoveryCtx *rcctx);
    void recovery_read_all_kv(RecoveryCtx *rcctx);
    void recovery_init_all_req_ctx(RecoveryCtx *rcctx);
    void recovery_redo_all_req(RecoveryCtx *rcctx);

    MemoryConsumption mem_con;

    void init_mem_con();
    void print_mem_con();

    int test_num;

    // add breakdown;
    bool if_req_latency;
    ReqLatency *req_latency;
    void client_init_req_latency(int num_ops, int op_type);
    void client_add_req_latency(ReqTimer *req_timer);
    void client_print_req_latency(char *op);

    // for rdma test
    void rdma_test_req_read(KVReqCtx *ctx);

    int race_insert(KVReqCtx *ctx);
        void race_insert_RTT1(KVReqCtx *ctx);
        void race_insert_RTT2(KVReqCtx *ctx);
    void *race_search(KVReqCtx *ctx);
        void race_search_RTT1(KVReqCtx *ctx);
        void race_search_RTT2(KVReqCtx *ctx);
    int race_update(KVReqCtx *ctx);
        void race_update_RTT1(KVReqCtx *ctx);
        void race_update_RTT2(KVReqCtx *ctx);
        void race_update_RTT3(KVReqCtx *ctx);
    int race_delete(KVReqCtx *ctx);
        void race_delete_RTT1(KVReqCtx *ctx);
        void race_delete_RTT2(KVReqCtx *ctx);
        void race_delete_RTT3(KVReqCtx *ctx);
};

typedef struct TagClientFiberArgs {
    Client   * client;
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

} ClientFiberArgs;

typedef struct TagEncodingThread{
    Client *client;
    KVReqCtx * ctx;
}EncodingThread;

void * client_ops_fb_cnt_time(void * arg);
void * client_ops_fb_cnt_ops_cont(void * arg);
void init_num_count(NumCount & num_count);
void add_num_count(NumCount & init_num_count, NumCount & new_num_count);
void print_num_count(NumCount code_num_count);
int get_all_num_failed(NumCount code_num_failed);

uint16_t combined_server_id(vector<uint8_t> & server_list);

#endif