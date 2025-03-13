#include "clientfr.h"

void ClientFR::print_error(const char * err){
    RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << err;
}

void ClientFR::print_mes(const char *mes){
    RDMA_LOG_IF(3, if_print_log) << "client:" << client_id << " " << mes;
}

void ClientFR::print_sp(const char *sp){
    RDMA_LOG_IF(4, if_print_log) << "client:" << client_id << " " << sp;
}

void ClientFR::print_args(const char *args_name, double value){
    RDMA_LOG_IF(3, if_print_log) << "client:" << client_id << " " << args_name << ": " << value;
}

ClientFR::ClientFR(struct GlobalConfig * conf) {
    lat_list = new uint64_t[MAX_TEST_NUM];
    pre_time = new timespec;
    gettimeofday(&recover_st_, NULL);
    is_recovery = conf->is_recovery;
    srand(time(nullptr));
    for(int i = 0;i < 6;i ++){
        num_count[i] = 0;
    }
    if_print_log = conf->if_print_log;
    client_id     = conf->server_id;
    if_batch = conf->if_batch;
    faa_need_value_WL = pow(2, 0);
    faa_need_value_RL = pow(2, 37);
    miss_rate_threash = conf->miss_rate_threash;
    num_cn = conf->num_cn;
    remote_global_meta_addr_ = conf->server_base_addr;
    remote_sync_addr = remote_global_meta_addr_ + sizeof(uint64_t);
    server_st_addr_     = conf->server_base_addr;
    server_data_len_    = conf->server_data_len;
    num_coroutines_   = conf->num_coroutines;
    main_core_id_ = conf->main_core_id;
    poll_core_id_ = conf->poll_core_id;
    encoding_core_id_ = conf->encoding_core_id;
    workload_run_time_ = conf->workload_run_time;
    num_total_operations_   = 0;
    num_local_operations_   = 0;
    kv_info_list_     = NULL;
    kv_req_ctx_list_  = NULL;
    num_memory_ = conf->memory_num;
    print_args("num MN", num_memory_);

    // network layer
    nm_ = new UDPNetworkManager(conf);
    print_mes("init network finished!");

    // connect network
    int ret = connect_ib_qps();
    print_mes("network test finished~");
    gettimeofday(&connection_recover_et_, NULL);

    // memory layer
    mm_ = new ClientFRMM(conf, nm_);
    print_mes("memory test finished~");
    gettimeofday(&mm_recover_et_, NULL);

    /*
        memory allocate, register RDMA
    */
    IbInfo ib_info;
    nm_->get_ib_info(&ib_info);
    size_t local_buf_sz = (size_t)CORO_LOCAL_BUF_LEN * num_coroutines_;
    // allocate bucket memory
    local_buf_ = mmap(NULL, local_buf_sz, PROT_READ | PROT_WRITE, 
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    while(local_buf_ == NULL){
        print_sp("local_buf_ allocate error, try again~");
        sleep(1);
        local_buf_ = mmap(NULL, local_buf_sz, PROT_READ | PROT_WRITE, 
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    }
    local_buf_mr_ = ibv_reg_mr(ib_info.ib_pd, local_buf_, local_buf_sz, 
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    while(local_buf_mr_ == NULL){
        print_sp("local_buf_mr_ allocate error, try again~");
        sleep(1);
        local_buf_mr_ = ibv_reg_mr(ib_info.ib_pd, local_buf_, local_buf_sz, 
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    }

    // allocate data memory
    int input_buf_len = CLINET_INPUT_BUF_LEN;
    input_buf_ = mmap(NULL, input_buf_len, PROT_READ | PROT_WRITE, 
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    while(input_buf_ == NULL){
        print_sp("input_buf_ allocate error, try again~");
        sleep(1);
        input_buf_ = mmap(NULL, input_buf_len, PROT_READ | PROT_WRITE, 
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    }
    input_buf_mr_ = ibv_reg_mr(ib_info.ib_pd, input_buf_, input_buf_len, 
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    while(input_buf_mr_ == NULL){
        print_sp("input_buf_mr_ allocate error, try again");
        sleep(1);
        input_buf_mr_ = ibv_reg_mr(ib_info.ib_pd, input_buf_, input_buf_len, 
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    }

    coro_local_addr_list_ = (uint64_t *)malloc(sizeof(uint64_t) * num_coroutines_);
    for (int i = 0; i < num_coroutines_; i ++) {
        coro_local_addr_list_[i] = (uint64_t)local_buf_ + CORO_LOCAL_BUF_LEN * i;
    }
    print_args("num coro", num_coroutines_);
    gettimeofday(&local_mr_reg_et_, NULL);

    // kv relation
    value_size = conf->value_size;
    key_size = conf->key_size;
    print_args("default value size", value_size);
    print_args("default key size", key_size);

    // hashring init
    k = conf->k_data;
    m = conf->m_parity;
    k_m = k + m;
    print_mes("ec paras~");
    print_args("k", k);
    print_args("m", m);
    hashring = new HashRing(NULL, k, m, conf->memory_num, conf->virtual_mn, conf->memory_ips);
    print_mes("hashring test finished~");

    // buffer_list
    buffer = new Buffer(k_m, conf->block_size, conf->subblock_size);
    print_mes("bufferrscoding test finished~");

    // init encoding struct
    init_encoding_mm_space();
    print_mes("encoding mm space~");
    max_stripe = conf->max_stripe;
    base_stripe_id = (client_id - num_memory_) * max_stripe;
    pre_stripe = base_stripe_id + 1;
    all_clients = conf->all_clients;
    is_use_cache = conf->is_use_cache;

    // thread pool
    // encoding_thread.init(1);
    // encoding_thread.start();
    // print_mes("encoding thread init~");
    print_mes("nice~");

    // add pre malloc sr lists
    init_sr_list();
    print_mes("per malloc sr list~");

    // init race root hash table
    gettimeofday(&ec_init_et_, NULL);
    init_all_hash_index();
    print_mes("Init client finished~");
}

ClientFR::~ClientFR() {
    delete nm_;
    // delete mm_;
}

void ClientFR::read_all_hash_table(){
    print_mes("get remote hash root~");
    int ret;
    // get all hash root
    ret = nm_->nm_rdma_read_from_sid((void *)hashring_table->kv_ec_meta_hash_root_, 
        hashring_table->kv_ec_meta_hash_root_mr_->lkey, ROOT_KV_META_LEN,
        remote_kv_ec_meta_root_addr_, server_mr_info_map_[0]->rkey, 0); 
    print_mes("get remote hash root finished~");
}

void ClientFR::write_all_hash_table(){
    print_mes("write all hash table~");
    int ret;
    for (int i = 0; i < num_memory_; i ++) {
        ret = nm_->nm_rdma_write_to_sid((void *)hashring_table->kv_ec_meta_hash_root_, 
            hashring_table->kv_ec_meta_hash_root_mr_->lkey, sizeof(HashRootKvEcMeta),
            remote_kv_ec_meta_root_addr_, server_mr_info_map_[i]->rkey, i);
    }
    print_mes("write all hash table finished~");
}

void ClientFR::init_all_hash_table_mem(){
    IbInfo ib_info;
    nm_->get_ib_info(&ib_info);
    // all hash table allocate
    hashring_table->kv_ec_meta_hash_root_ = (HashRootKvEcMeta *) malloc (ROOT_KV_META_LEN);
    hashring_table->kv_ec_meta_hash_root_mr_ = ibv_reg_mr(ib_info.ib_pd, hashring_table->kv_ec_meta_hash_root_,
        ROOT_KV_META_LEN, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    remote_kv_ec_meta_root_addr_ = remote_global_meta_addr_ + META_AREA_LEN + GC_AREA_LEN;
    remote_object_index_addr = remote_kv_ec_meta_root_addr_ + ROOT_KV_META_LEN;
    remote_stripe_index_addr = remote_object_index_addr + MAX_OBJ_LEN;
    remote_chunk_index_addr  = remote_stripe_index_addr + MAX_STR_LEN;
}

int ClientFR::init_all_hash_index(){
    hashring_table = new HashRingTable;
    int ret;
    init_all_hash_table_mem();
    if (client_id - num_memory_ == 0) {
        read_all_hash_table();
        ret = init_all_hash_subtable();
        write_all_hash_table();
        ret = sync_init_finish();
    } else {
        while (!init_is_finished());
        read_all_hash_table();
    }
    print_mes("Init all subtable finished~");
}

void ClientFR::init_sr_list(){
    sr_list_queue = new CircularQueue<IbvSrList *>(MAX_SR_LIST);
    sr_queue      = new CircularQueue<ibv_send_wr *>(MAX_SR_LIST);
    sge_queue     = new CircularQueue<ibv_sge *>(MAX_SR_LIST);
    double_sr_queue = new CircularQueue<ibv_send_wr *>(MAX_SR_LIST);
    double_sge_queue = new CircularQueue<ibv_sge *>(MAX_SR_LIST);
    IbvSrList * sr_list;
    struct ibv_send_wr * sr;
    struct ibv_sge * sge;
    for(int i = 0;i < MAX_SR_LIST;i ++){
        sr_list = (IbvSrList *)malloc(sizeof(IbvSrList));
        sr_list_queue->enqueue(sr_list);
        sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr));
        sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge));
        memset(sr, 0, sizeof(struct ibv_send_wr));
        memset(sge, 0, sizeof(struct ibv_sge));
        sr_queue->enqueue(sr);
        sge_queue->enqueue(sge);
        sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * 2);
        sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * 2);
        memset(sr, 0, sizeof(struct ibv_send_wr) * 2);
        memset(sge, 0, sizeof(struct ibv_sge) * 2);
        double_sr_queue->enqueue(sr);
        double_sge_queue->enqueue(sge);
    }
}

IbvSrList *ClientFR::get_one_sr_list(){
    return sr_list_queue->dequeue_free();
}

ibv_send_wr *ClientFR::get_one_sr(){
    return sr_queue->dequeue_free();
}

ibv_sge *ClientFR::get_one_sge(){
    return sge_queue->dequeue_free();
}

ibv_send_wr *ClientFR::get_double_sr(){
    return double_sr_queue->dequeue_free();
}

ibv_sge *ClientFR::get_double_sge(){
    return double_sge_queue->dequeue_free();
}

IbvSrList *ClientFR::get_one_sr_list_encoding(){
    return ectx->sr_list_queue->dequeue_free();
}

ibv_send_wr *ClientFR::get_one_sr_encoding(){
    return ectx->sr_queue->dequeue_free();
}

ibv_sge *ClientFR::get_one_sge_encoding(){
    return ectx->sge_queue->dequeue_free();
}

ibv_send_wr *ClientFR::get_double_sr_encoding(){
    return ectx->double_sr_queue->dequeue_free();
}

ibv_sge *ClientFR::get_double_sge_encoding(){
    return ectx->double_sge_queue->dequeue_free();
}

int ClientFR::connect_ib_qps() {
    uint32_t num_servers = nm_->get_num_servers();
    for (int i = 0; i < num_servers; i ++) {
        struct MrInfo * gc_info = (struct MrInfo *)malloc(sizeof(struct MrInfo));
        nm_->client_connect_one_rc_qp(i, gc_info);
        server_mr_info_map_[i] = gc_info;
    }
    return 0;
}

void ClientFR::mm_alloc_to_subtable_entry(RaceHashSubtableEntry & entry, ClientMMAllocSubtableCtx & mmctx){
    entry.local_depth = num_memory_;
    entry.lock = 0;
    entry.server_id = mmctx.server_id;
    hash_index_convert_64_to_40_bits(mmctx.addr, entry.pointer);
}

int ClientFR::init_all_hash_subtable(){
    print_mes("init all hash subtable~");
    SmallHashRing *small_hashring_primary;
    SmallHashRing *small_hashring_backup[k_m];
    uint8_t primary_server;
    uint8_t backup_server[k_m];
    vector<uint8_t> backup_num_mn;
    vector<uint8_t> backup_mn_index;
    int all_com_backup_mn;
    int pre_mn;
    ClientMMAllocSubtableCtx subtable_info;

    // kv ec meta hashtable
    for(int i = 0;i < k_m;i ++){
        small_hashring_primary = &hashring->small_hashring[i];
        for(int j = 0;j < small_hashring_primary->num_mn;j ++){
            primary_server = small_hashring_primary->mn_id[j];
            // mm alloc big subtable primary server
            all_com_backup_mn = 1;
            backup_num_mn.clear();
            for(int k = 0;k < m;k ++){
                small_hashring_backup[k] = &hashring->small_hashring[(i + 1 + k) % k_m];
                pre_mn = small_hashring_backup[k]->num_mn;
                all_com_backup_mn *= pre_mn;
                backup_num_mn.push_back(pre_mn);
            }
            backup_mn_index.clear();
            for(int k = 0;k < all_com_backup_mn;k ++){
                // mm alloc big subtable primary server
                mm_->mm_alloc_big_subtable(nm_, &subtable_info, primary_server);
                mm_alloc_to_subtable_entry(hashring_table->kv_ec_meta_hash_root_->
                    subtable_entry[primary_server][k].subtable_entry_primary, subtable_info);
                // backup
                backup_mn_index = get_multidimensional_index(backup_num_mn, k);
                for(int l = 0;l < m;l ++){
                    backup_server[l] = small_hashring_backup[l]->mn_id[backup_mn_index[l]];
                    // mm alloc small subtable backup server
                    mm_->mm_alloc_small_subtable(nm_, &subtable_info, backup_server[l]);
                    mm_alloc_to_subtable_entry(hashring_table->kv_ec_meta_hash_root_
                        ->subtable_entry[primary_server][k].subtable_entry_rep[l], subtable_info);
                }
            }
        }
    }

    print_mes("init all hash subtable finished~");
}

bool ClientFR::init_is_finished() {
    nm_->nm_rdma_read_from_sid(local_buf_, local_buf_mr_->lkey, 
        sizeof(uint64_t), remote_global_meta_addr_, server_mr_info_map_[0]->rkey, 0);
    uint64_t read_value = *(uint64_t *)local_buf_;
    return read_value == 1;
}

int ClientFR::sync_init_finish() {
    print_mes("sync~");
    uint64_t local_msg = 1;
    nm_->nm_rdma_write_inl_to_sid(&local_msg, sizeof(uint64_t), 
        remote_global_meta_addr_, server_mr_info_map_[0]->rkey, 0);
    print_mes("sync finished~");
    return 0;
}

bool ClientFR::test_sync_faa_async(){
    int ret = 0;
    struct ibv_send_wr sr;
    struct ibv_sge     sge;
    memset(&sge, 0, sizeof(struct ibv_sge));
    sge.addr   = (uint64_t)local_buf_;
    sge.length = sizeof(uint64_t);
    sge.lkey   = local_buf_mr_->lkey;
    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id   = 101;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_ATOMIC_FETCH_AND_ADD;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.atomic.remote_addr = remote_sync_addr;
    sr.wr.atomic.rkey = server_mr_info_map_[0]->rkey;
    sr.wr.atomic.compare_add = 1;
    IbvSrList *sr_list = new IbvSrList;
    sr_list->num_sr = 1;
    sr_list->server_id = 0;
    sr_list->sr_list = &sr;
    std::map<uint64_t, bool> comp_wrid_map;
    send_one_sr_list(sr_list, &comp_wrid_map);
    poll_completion(comp_wrid_map);
    return 0;
}

bool ClientFR::test_sync_read_async(){
    int ret = 0;
    struct ibv_send_wr sr;
    struct ibv_sge     sge;
    memset(&sge, 0, sizeof(struct ibv_sge));
    sge.addr   = (uint64_t)local_buf_;
    sge.length = sizeof(uint64_t);
    sge.lkey   = local_buf_mr_->lkey;
    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id = 101;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_RDMA_READ;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.rdma.remote_addr = remote_sync_addr;
    sr.wr.rdma.rkey = server_mr_info_map_[0]->rkey;
    std::map<uint64_t, bool> comp_wrid_map;
    IbvSrList *sr_list = new IbvSrList;
    sr_list->num_sr = 1;
    sr_list->server_id = 0;
    sr_list->sr_list = &sr;
    while(1){
        comp_wrid_map.clear();
        send_one_sr_list(sr_list, &comp_wrid_map);
        poll_completion(comp_wrid_map);
        uint64_t read_value = *(uint64_t *)local_buf_;
        if (read_value == all_clients * num_cn) {
            break;
        }
    }
    return true;
}

bool ClientFR::test_sync_read_async_double(){
    int ret = 0;
    struct ibv_send_wr sr;
    struct ibv_sge     sge;
    memset(&sge, 0, sizeof(struct ibv_sge));
    sge.addr   = (uint64_t)local_buf_;
    sge.length = sizeof(uint64_t);
    sge.lkey   = local_buf_mr_->lkey;
    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id = 101;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_RDMA_READ;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.rdma.remote_addr = remote_sync_addr;
    sr.wr.rdma.rkey = server_mr_info_map_[0]->rkey;
    std::map<uint64_t, bool> comp_wrid_map;
    IbvSrList *sr_list = new IbvSrList;
    sr_list->num_sr = 1;
    sr_list->server_id = 0;
    sr_list->sr_list = &sr;
    while(1){
        comp_wrid_map.clear();
        send_one_sr_list(sr_list, &comp_wrid_map);
        poll_completion(comp_wrid_map);
        uint64_t read_value = *(uint64_t *)local_buf_;
        if (read_value == all_clients * num_cn * 2) {
            break;
        }
    }
    return true;
}

void ClientFR::init_kv_req_ctx(KVReqCtx * ctx, char * operation) {
    ctx->lkey = local_buf_mr_->lkey;
    ctx->kv_modify_pr_cas_list.resize(1);
    ctx->ret_val.ret_code = 0;
    char key_buf[128] = {0};
    ::memcpy(key_buf, (void *)((uint64_t)(ctx->kv_info->l_addr) + sizeof(KvKeyLen) + 
        sizeof(KvValueLen)), ctx->kv_info->key_len);
    ctx->key_str = std::string(key_buf);
    if (strcmp(operation, "INSERT") == 0) {
        ctx->req_type = KV_REQ_INSERT;
    } else if (strcmp(operation, "DELETE") == 0) {
        ctx->req_type = KV_REQ_DELETE;
    } else if (strcmp(operation, "UPDATE") == 0) {
        ctx->req_type = KV_REQ_UPDATE;
    } else if (strcmp(operation, "SEARCH") == 0) {
        ctx->req_type = KV_REQ_SEARCH;
    } else if (strcmp(operation, "DEGRADE") == 0){
        ctx->req_type = KV_DEGRADE_READ;
    } else {
        ctx->req_type = KV_REQ_SEARCH;
    }
}

void ClientFR::init_kv_req_ctx_ycsb(KVReqCtx * ctx, char * operation){
    ctx->lkey = local_buf_mr_->lkey;
    ctx->kv_modify_pr_cas_list.resize(1);
    int key_s = ctx->kv_info->key_len;
    char key_buf[key_s];
    memcpy(key_buf, (void *)((uint64_t)(ctx->kv_info->l_addr) + KV_KEY_LEN + KV_VALUE_LEN), key_s);
    ctx->key_str = string(key_buf);
    if (strcmp(operation, "INSERT") == 0) {
        ctx->req_type = KV_REQ_INSERT;
    } else if (strcmp(operation, "DELETE") == 0) {
        ctx->req_type = KV_REQ_DELETE;
    } else if (strcmp(operation, "UPDATE") == 0) {
        ctx->req_type = KV_REQ_UPDATE;
    } else if (strcmp(operation, "READ") == 0) {
        ctx->req_type = KV_REQ_SEARCH;
    } else if (strcmp(operation, "DEGRADE") == 0){
        ctx->req_type = KV_DEGRADE_READ;
    } else {
        ctx->req_type = KV_REQ_SEARCH;
    }
}

void ClientFR::init_kv_insert_space(void * coro_local_addr, KVReqCtx * ctx) {
    ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 2 * sizeof(KvEcMetaBucket));
    ctx->local_cas_return_value_addr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t));
    ctx->op_laddr = (void *)((uint64_t)ctx->local_cas_return_value_addr + sizeof(uint64_t));
    ctx->insert_kv_log_buf_ = (LogEntry *)ctx->op_laddr;
    ctx->insert_faa_wp_return_addr = (void *)((uint64_t)ctx->insert_kv_log_buf_ + LOG_ENTRY_LEN);
}

void ClientFR::init_kv_insert_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_insert_space(coro_local_addr, ctx);
}

void ClientFR::init_kv_search_space(void * coro_local_addr, KVReqCtx * ctx) {
    ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    ctx->local_kv_addr     = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
}

void ClientFR::init_kv_search_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_search_space(coro_local_addr, ctx);
}

void ClientFR::init_kv_update_space(void * coro_local_addr, KVReqCtx * ctx) {
    ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 16 * sizeof(RaceHashBucket));
    ctx->local_cas_return_value_addr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t));
    ctx->op_laddr = (void *)((uint64_t)ctx->local_cas_return_value_addr + sizeof(uint64_t));
    ctx->local_kv_addr = ctx->op_laddr;
    UpdateCtx *uctx = ctx->uctx;

    uint64_t ptr = (uint64_t)ctx->local_kv_addr + get_aligned_size(ctx->kv_info->key_len + 
        ctx->kv_info->value_len + KV_OTHER_LEN, mm_->subblock_sz_);

    uctx->object_index_addr = ptr;
    ptr += OBJ_INDEX_LEN;
    uctx->stripe_index_addr = ptr;
    ptr += STR_INDEX_LEN;
    for(int i = 0;i < m;i ++){
        uctx->parity_chunk_addr[i] = ptr;
        ptr += buffer->block_size;
    }
}

void ClientFR::init_kv_update_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_update_space(coro_local_addr, ctx);
}

void ClientFR::init_kv_delete_space(void * coro_local_addr, KVReqCtx * ctx) {
    ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 16 * sizeof(RaceHashBucket));
    ctx->local_cas_return_value_addr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t));
    ctx->op_laddr = (void *)((uint64_t)ctx->local_cas_return_value_addr + sizeof(uint64_t));
    ctx->local_kv_addr = ctx->op_laddr;
    ctx->local_cache_addr = ctx->local_kv_addr;
    uint64_t ptr = (uint64_t)ctx->local_kv_addr + 3 * get_aligned_size(ctx->kv_info->key_len + 
        ctx->kv_info->value_len + KV_OTHER_LEN, mm_->subblock_sz_);
    DeleteCtx *dctx = ctx->dctx;
    dctx->object_index_addr = ptr;
    ptr += OBJ_INDEX_LEN;
    dctx->stripe_index_addr = ptr;
    ptr += STR_INDEX_LEN;
    for(int i = 0;i < m;i ++){
        dctx->parity_chunk_addr[i] = ptr;
        ptr += buffer->block_size;
    }
    dctx->object_0_addr = ptr;
    ptr += OBJ_INDEX_LEN;
}

void ClientFR::init_kv_delete_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_delete_space(coro_local_addr, ctx);
}

void ClientFR::init_kv_degrade_read_space(void * coro_local_addr, KVReqCtx * ctx) {
    ctx->use_cache = true;
    ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    ctx->local_cache_addr = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
    ctx->local_kv_addr = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
    ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
    ctx->local_cas_return_value_addr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t));
    ctx->op_laddr = (void *)((uint64_t)ctx->local_cas_return_value_addr + sizeof(uint64_t));
    DegradeReadCtx *drctx = ctx->drctx;
    uint64_t ptr = (uint64_t)ctx->op_laddr;
    drctx->object_index_addr = ptr;
    ptr += OBJ_INDEX_LEN;
    drctx->stripe_index_addr = ptr;
    ptr += STR_INDEX_LEN;
    for(int i = 0;i < m;i ++){
        drctx->parity_chunk_addr[i] = ptr;
        ptr += buffer->block_size;
    }
    drctx->related_kv_addr = new uint64_t[k];
    for(int i = 0;i < k;i ++){
        drctx->related_kv_addr[i] = ptr;
        ptr += mm_->subblock_sz_;
    }
    drctx->data = new u8*[k_m];
}

void ClientFR::init_kv_degrade_read_space(void * coro_local_addr, uint32_t kv_req_idx){
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_degrade_read_space(coro_local_addr, ctx);
}

void ClientFR::init_bucket_space(BucketBuffer *bucketbuffer, uint64_t & ptr){
    bucketbuffer->local_bucket_addr = (RaceHashBucket *)ptr;
    ptr += sizeof(RaceHashBucket) * 4;
    bucketbuffer->local_cas_return_value_addr = (void *)ptr;
    ptr += sizeof(uint64_t);
}

void ClientFR::init_bucket_space_big_slot(BucketBuffer *bucketbuffer, uint64_t & ptr){
    bucketbuffer->local_bucket_addr = (RaceHashBucket *)ptr;
    ptr += sizeof(KvEcMetaBucket) * 4;
    bucketbuffer->local_cas_return_value_addr = (void *)ptr;
    ptr += sizeof(uint64_t);
}

void ClientFR::init_kvreq_space(uint32_t coro_id, uint32_t kv_req_st_idx, uint32_t num_ops) { 
    void * coro_local_addr = (void *)coro_local_addr_list_[coro_id];
    for (uint32_t i = 0; i < num_ops; i ++) {
        uint32_t kv_req_idx = kv_req_st_idx + i;
        kv_req_ctx_list_[kv_req_idx].coro_id = coro_id;
        switch (kv_req_ctx_list_[kv_req_idx].req_type) {
        case KV_REQ_INSERT:
            init_kv_insert_space(coro_local_addr, kv_req_idx);
            break;
        case KV_REQ_SEARCH:
            init_kv_search_space(coro_local_addr, kv_req_idx);
            break;
        case KV_REQ_UPDATE:
            init_kv_update_space(coro_local_addr, kv_req_idx);
            break;
        case KV_REQ_DELETE:
            init_kv_delete_space(coro_local_addr, kv_req_idx);
            break;
        case KV_DEGRADE_READ:
            init_kv_degrade_read_space(coro_local_addr, kv_req_idx);
            break;
        default:
            kv_req_ctx_list_[kv_req_idx].req_type = KV_REQ_SEARCH;
            init_kv_search_space(coro_local_addr, kv_req_idx);
            break;
        }
        prepare_request(&kv_req_ctx_list_[kv_req_idx]);
    }
}

void ClientFR::load_kv_req_init_update(KVReqCtx *ctx){
    ctx->ret_val.ret_code = KV_OPS_SUCCESS;
    ctx->uctx = new UpdateCtx;
    UpdateCtx *uctx = ctx->uctx;
    uctx->parity_chunk_addr = new uint64_t[m];
    ctx->is_finished = false;
}

void ClientFR::load_kv_req_init_delete(KVReqCtx *ctx){
    ctx->ret_val.ret_code = 0;
    ctx->dctx = new DeleteCtx;
    DeleteCtx *dctx = ctx->dctx;
    dctx->parity_chunk_addr = new uint64_t[m];
    ctx->is_finished = false;
}

void ClientFR::load_kv_req_init_degrade_read(KVReqCtx * ctx){
    ctx->ret_val.ret_code = 0;
    ctx->drctx = new DegradeReadCtx;
    DegradeReadCtx *drctx = ctx->drctx;
    drctx->parity_chunk_addr = new uint64_t[m];
    ctx->is_finished = false;
}

int ClientFR::load_kv_req_micro_latency(int num_op, const char * op){
    if (num_total_operations_ != 0) {
        delete [] kv_info_list_;
        delete [] kv_req_ctx_list_;
        num_total_operations_ = 0;
        num_local_operations_ = 0;
    }
    char operation_buf[16];
    char key_buf[key_size];
    char value_buf[value_size];
    unsigned char key_len_buf[2] = {0};
    unsigned char value_len_buf[4] = {0};
    unsigned char crc_buf[1] = {0};
    int crc = 0;
    int leave_size;
    queue<string> update_trace_queue;
    queue<string> delete_trace_queue;
    queue<string> degrade_trace_queue;
    if(strcmp(op, "UPDATE") == 0){
        for(int i = 0;i < k * (buffer->block_size) / mm_->subblock_sz_;i ++){
            for(int j = 0;j < ectx->stripe_id - 1 - base_stripe_id;j ++){
                update_trace_queue.push(update_trace_key_list_per_stripe[j].front());
                update_trace_key_list_per_stripe[j].pop_front();
            }
        }
        num_total_operations_ = update_trace_queue.size();
        // num_total_operations_ = 1;
    } else if(strcmp(op, "DELETE") == 0){
        for(int i = 0;i < k * (buffer->block_size) / mm_->subblock_sz_;i ++){
            for(int j = 0;j < ectx->stripe_id - 1;j ++){
                delete_trace_queue.push(delete_trace_key_list_per_stripe[j].front());
                delete_trace_key_list_per_stripe[j].pop_front();
            }
        }
        num_total_operations_ = delete_trace_queue.size();
        // num_total_operations_ = 1;
    } else if(strcmp(op, "DEGRADE") == 0){
        num_op = (buffer->block_size / mm_->subblock_sz_) * k;
        for(int i = 0;i < num_op;i ++){
            for(int j = 0;j < ectx->stripe_id - 1;j ++){
                degrade_trace_queue.push(degrade_read_trace_key_list_per_stripe[j].front());
                degrade_read_trace_key_list_per_stripe[j].pop_front();
            }
        }
        num_total_operations_ = degrade_trace_queue.size();
        // num_total_operations_ = 100;
    } else {
        num_total_operations_ = num_op;
    }
    // num_total_operations_ 
    num_local_operations_ = num_total_operations_;
    RDMA_LOG_IF(3, if_print_log) << "server:" << client_id << " " << "load "<< num_local_operations_ << " operations"; 
    kv_info_list_    = new KVInfo[num_local_operations_];
    kv_req_ctx_list_ = new KVReqCtx[num_local_operations_];
    if (kv_info_list_ == NULL || kv_req_ctx_list_ == NULL) {
        printf("failed to allocate kv_info_list or kv_req_ctx_list\n");
        abort();
    }
    memset(kv_info_list_, 0, sizeof(KVInfo) * num_local_operations_);
    // memset(kv_req_ctx_list_, 0, sizeof(KVReqCtx) * num_local_operations_);
    uint64_t input_buf_ptr = (uint64_t)input_buf_;
    uint32_t used_len = 0;
    const char* delimiter_key = "-";
    const char* delimiter_value = "-laitini-";
    for (int i = 0; i < num_local_operations_; i ++) {
        sprintf(operation_buf, op);
        if(strcmp(operation_buf, "UPDATE") == 0){
            sprintf(key_buf, update_trace_queue.front().c_str());
            update_trace_queue.pop();
            sprintf(value_buf, "initial-value--%d", i);
        } else if(strcmp(operation_buf, "DELETE") == 0){
            sprintf(key_buf, delete_trace_queue.front().c_str());
            delete_trace_queue.pop();
            memset(value_buf, 0, value_size);
        } else if(strcmp(operation_buf, "DEGRADE") == 0){
            ::memcpy(key_buf, degrade_trace_queue.front().c_str(), key_size);
            degrade_trace_queue.pop();
            sprintf(value_buf, "initial-value--%d", i);
        } else {
            convertToBase62(i, key_size, client_id, delimiter_key, key_buf);
            convertToBase62(i, value_size, client_id, delimiter_value, value_buf);
        }
        // record the key and value
        uint32_t all_len = sizeof(KvKeyLen) + sizeof(KvValueLen) + key_size
            + value_size + sizeof(KvTail);
        KvKeyLen * kvkeylen = (KvKeyLen *)input_buf_ptr;
        KvValueLen * kvvaluelen = (KvValueLen *)(input_buf_ptr + sizeof(KvKeyLen));
        void * key_st_addr = (void *)(input_buf_ptr + sizeof(KvKeyLen) + sizeof(KvValueLen));
        void * value_st_addr = (void *)((uint64_t)key_st_addr + key_size);
        KvTail * kvtail = (KvTail *)((uint64_t)input_buf_ptr + 
        sizeof(KvKeyLen) + sizeof(KvValueLen) + key_size + value_size);
        ::memcpy(key_st_addr, key_buf, key_size);
        ::memcpy(value_st_addr, value_buf, value_size);
        int len = sizeof(KvKeyLen);
        int value = key_size;
        int_to_char(value, key_len_buf, len);
        ::memcpy(kvkeylen, key_len_buf, sizeof(key_len_buf));
        len = sizeof(KvValueLen);
        value = value_size;
        int_to_char(value, value_len_buf, len);
        ::memcpy(kvvaluelen, value_len_buf, sizeof(value_len_buf));
        len = sizeof(KvTail);
        value = crc;
        int_to_char(value, crc_buf, len);
        ::memcpy(kvtail, crc_buf, sizeof(crc_buf));
        kv_info_list_[i].key_len = key_size;
        kv_info_list_[i].value_len = value_size;
        kv_info_list_[i].l_addr  = (void *)input_buf_ptr;
        kv_info_list_[i].lkey = input_buf_mr_->lkey;
        kv_req_ctx_list_[i].kv_all_len = all_len;
        kv_req_ctx_list_[i].kv_info = &kv_info_list_[i];
        input_buf_ptr += all_len; 
        used_len += all_len;
        if(strcmp(operation_buf, "UPDATE") == 0){
            load_kv_req_init_update(&kv_req_ctx_list_[i]);
        } else if(strcmp(operation_buf, "DELETE") == 0){
            load_kv_req_init_delete(&kv_req_ctx_list_[i]);
        } else if(strcmp(operation_buf, "DEGRADE") == 0){
            load_kv_req_init_degrade_read(&kv_req_ctx_list_[i]);
        }
        if (used_len >= CLINET_INPUT_BUF_LEN) {
            printf("overflow!\n");
        }
        // record operation
        init_kv_req_ctx(&kv_req_ctx_list_[i], operation_buf);
    }
    print_mes("load finished~");
}

int ClientFR::load_kv_req_from_file_ycsb(const char * fname, uint32_t st_idx, int32_t num_ops){
    RDMA_LOG_IF(3, if_print_log) << "load " << st_idx << " " << num_ops;
    int ret = 0;
    FILE * workload_file = fopen(fname, "r");
    if(workload_file == NULL){
        RDMA_LOG_IF(2, if_print_log) << "failed to open: " << fname;
        return -1;
    }
    // clear pre struct
    if(num_total_operations_ != 0){
        delete [] kv_info_list_;
        delete [] kv_req_ctx_list_;
        num_total_operations_ = 0;
        num_local_operations_ = 0;
    }
    int other_size = sizeof(KvKeyLen) + sizeof(KvValueLen) + sizeof(KvTail);
    int pre_value_size = value_size - other_size - key_size;
    char operation_buf[16];
    char table_buf[16];
    char key_buf[key_size];
    char value_buf[pre_value_size];
    unsigned char key_len_buf[2] = {0};
    unsigned char value_len_buf[4] = {0};
    unsigned char crc_buf[1] = {0};
    int crc = 0;
    while (fscanf(workload_file, "%s %s %s", operation_buf, table_buf, key_buf) != EOF) {
        num_total_operations_ ++;
    }
    if (num_ops == -1) {
        num_local_operations_ = num_total_operations_;
    } else {
        num_local_operations_ = (st_idx + num_ops > num_total_operations_) ? num_total_operations_ - st_idx : num_ops;
    }
    print_args("load operations:", num_local_operations_);
    kv_info_list_    = new KVInfo[num_local_operations_];
    kv_req_ctx_list_ = new KVReqCtx[num_local_operations_];
    if (kv_info_list_ == NULL || kv_req_ctx_list_ == NULL) {
        printf("failed to allocate kv_info_list or kv_req_ctx_list\n");
        abort();
    }
    uint64_t input_buf_ptr = (uint64_t)input_buf_;
    rewind(workload_file);
    uint64_t used_len = 0;
    int leave_key, leave_value;
    const char* delimiter_value = "-laitini-";
    for(int i = 0;i < st_idx + num_local_operations_;i ++){
        ret = fscanf(workload_file, "%s %s %s", operation_buf, table_buf, key_buf);
        if(i < st_idx){
            continue;
        }
        leave_key = key_size - strlen(key_buf);
        memset(key_buf + strlen(key_buf), 0, leave_key);
        convertToBase62(i, pre_value_size, client_id, delimiter_value, value_buf);
        // record the key and value
        uint32_t all_len = value_size;
        KvKeyLen * kvkeylen = (KvKeyLen *)input_buf_ptr;
        KvValueLen * kvvaluelen = (KvValueLen *)(input_buf_ptr + sizeof(KvKeyLen));
        void * key_st_addr = (void *)(input_buf_ptr + sizeof(KvKeyLen) + sizeof(KvValueLen));
        void * value_st_addr = (void *)((uint64_t)key_st_addr + key_size);
        KvTail * kvtail = (KvTail *)((uint64_t)input_buf_ptr + 
        sizeof(KvKeyLen) + sizeof(KvValueLen) + key_size + pre_value_size);
        memcpy(key_st_addr, key_buf, key_size);
        memcpy(value_st_addr, value_buf, pre_value_size);
        int len = sizeof(KvKeyLen);
        int value = key_size;
        int_to_char(value, key_len_buf, len);
        memcpy(kvkeylen, key_len_buf, sizeof(key_len_buf));
        len = sizeof(KvValueLen);
        value = pre_value_size;
        int_to_char(value, value_len_buf, len);
        memcpy(kvvaluelen, value_len_buf, sizeof(value_len_buf));
        len = sizeof(KvTail);
        value = crc;
        int_to_char(value, crc_buf, len);
        memcpy(kvtail, crc_buf, sizeof(crc_buf));
        kv_info_list_[i].key_len = key_size;
        kv_info_list_[i].value_len = pre_value_size;
        kv_info_list_[i].l_addr  = (void *)input_buf_ptr;
        kv_info_list_[i].lkey = input_buf_mr_->lkey;
        kv_req_ctx_list_[i].kv_all_len = all_len;
        kv_req_ctx_list_[i].kv_info = &kv_info_list_[i];
        input_buf_ptr += all_len; 
        used_len += all_len;
        if(strcmp(operation_buf, "UPDATE") == 0){
            load_kv_req_init_update(&kv_req_ctx_list_[i]);
        } else if(strcmp(operation_buf, "DELETE") == 0){
            load_kv_req_init_delete(&kv_req_ctx_list_[i]);
        } else if(strcmp(operation_buf, "DEGRADE") == 0){
            load_kv_req_init_degrade_read(&kv_req_ctx_list_[i]);
        }
        if (used_len >= CLINET_INPUT_BUF_LEN) {
            printf("overflow!\n");
        }
        // record operation
        init_kv_req_ctx_ycsb(&kv_req_ctx_list_[i], operation_buf);
    }
    print_mes("load finished~");
    fclose(workload_file);
}

int ClientFR::load_kv_req_file_mncrashed(const char * fname, uint32_t st_idx, int32_t num_ops){
    RDMA_LOG_IF(3, if_print_log) << "load " << st_idx << " " << num_ops;
    int ret = 0;
    FILE * workload_file = fopen(fname, "r");
    if(workload_file == NULL){
        RDMA_LOG_IF(2, if_print_log) << "failed to open: " << fname;
        return -1;
    }
    // clear pre struct
    if(num_total_operations_ != 0){
        delete [] kv_info_list_;
        delete [] kv_req_ctx_list_;
        num_total_operations_ = 0;
        num_local_operations_ = 0;
    }
    int other_size = sizeof(KvKeyLen) + sizeof(KvValueLen) + sizeof(KvTail);
    int pre_value_size = value_size - other_size - key_size;
    char operation_buf[16];
    char table_buf[16];
    char key_buf[key_size];
    char value_buf[pre_value_size];
    unsigned char key_len_buf[2] = {0};
    unsigned char value_len_buf[4] = {0};
    unsigned char crc_buf[1] = {0};
    int crc = 0;
    while (fscanf(workload_file, "%s %s %s", operation_buf, table_buf, key_buf) != EOF) {
        num_total_operations_ ++;
    }
    if (num_ops == -1) {
        num_local_operations_ = num_total_operations_;
    } else {
        num_local_operations_ = (st_idx + num_ops > num_total_operations_) ? num_total_operations_ - st_idx : num_ops;
    }
    print_args("load operations:", num_local_operations_);
    kv_info_list_    = new KVInfo[num_local_operations_];
    kv_req_ctx_list_ = new KVReqCtx[num_local_operations_];
    if (kv_info_list_ == NULL || kv_req_ctx_list_ == NULL) {
        printf("failed to allocate kv_info_list or kv_req_ctx_list\n");
        abort();
    }
    uint64_t input_buf_ptr = (uint64_t)input_buf_;
    rewind(workload_file);
    uint64_t used_len = 0;
    int leave_key, leave_value;
    const char* delimiter_value = "-laitini-";
    for(int i = 0;i < st_idx + num_local_operations_;i ++){
        ret = fscanf(workload_file, "%s %s %s", operation_buf, table_buf, key_buf);
        if(i < st_idx){
            continue;
        }
        if(i % k_m == 0){
            sprintf(operation_buf, "DEGRADE");
        }
        leave_key = key_size - strlen(key_buf);
        memset(key_buf + strlen(key_buf), 0, leave_key);
        convertToBase62(i, pre_value_size, client_id, delimiter_value, value_buf);
        // record the key and value
        uint32_t all_len = value_size;
        KvKeyLen * kvkeylen = (KvKeyLen *)input_buf_ptr;
        KvValueLen * kvvaluelen = (KvValueLen *)(input_buf_ptr + sizeof(KvKeyLen));
        void * key_st_addr = (void *)(input_buf_ptr + sizeof(KvKeyLen) + sizeof(KvValueLen));
        void * value_st_addr = (void *)((uint64_t)key_st_addr + key_size);
        KvTail * kvtail = (KvTail *)((uint64_t)input_buf_ptr + 
        sizeof(KvKeyLen) + sizeof(KvValueLen) + key_size + pre_value_size);
        memcpy(key_st_addr, key_buf, key_size);
        memcpy(value_st_addr, value_buf, pre_value_size);
        int len = sizeof(KvKeyLen);
        int value = key_size;
        int_to_char(value, key_len_buf, len);
        memcpy(kvkeylen, key_len_buf, sizeof(key_len_buf));
        len = sizeof(KvValueLen);
        value = pre_value_size;
        int_to_char(value, value_len_buf, len);
        memcpy(kvvaluelen, value_len_buf, sizeof(value_len_buf));
        len = sizeof(KvTail);
        value = crc;
        int_to_char(value, crc_buf, len);
        memcpy(kvtail, crc_buf, sizeof(crc_buf));
        kv_info_list_[i].key_len = key_size;
        kv_info_list_[i].value_len = pre_value_size;
        kv_info_list_[i].l_addr  = (void *)input_buf_ptr;
        kv_info_list_[i].lkey = input_buf_mr_->lkey;
        kv_req_ctx_list_[i].kv_all_len = all_len;
        kv_req_ctx_list_[i].kv_info = &kv_info_list_[i];
        input_buf_ptr += all_len; 
        used_len += all_len;
        if(strcmp(operation_buf, "UPDATE") == 0){
            load_kv_req_init_update(&kv_req_ctx_list_[i]);
        } else if(strcmp(operation_buf, "DELETE") == 0){
            load_kv_req_init_delete(&kv_req_ctx_list_[i]);
        } else if(strcmp(operation_buf, "DEGRADE") == 0){
            load_kv_req_init_degrade_read(&kv_req_ctx_list_[i]);
        }
        if (used_len >= CLINET_INPUT_BUF_LEN) {
            printf("overflow!\n");
        }
        // record operation
        init_kv_req_ctx_ycsb(&kv_req_ctx_list_[i], operation_buf);
    }
    print_mes("load finished~");
    fclose(workload_file);
}

void ClientFR::hash_compute_fp(KVHashInfo * hash_info){
    hash_info->fp = hash_index_compute_fp(hash_info->hash_value);
}

void ClientFR::get_kv_hash_info(KVInfo * kv_info, __OUT KVHashInfo * hash_info) {
    uint64_t key_addr = (uint64_t)kv_info->l_addr + KV_KEY_LEN + KV_VALUE_LEN;
    hash_info->hash_value = VariableLengthHash((void *)key_addr, kv_info->key_len, 0);
    hash_compute_fp(hash_info);
}

void ClientFR::get_rep_server_subtable_addr(KVHashInfo * hash_info, KVTableAddrInfo *addr_info, uint8_t type){
    uint64_t hash_value = hash_info->hash_value;
    vector<uint8_t> mn_index;
    vector<uint8_t> num_mn;
    int hash_root_index;
    uint8_t ring_id, primary_server_id;
    primary_server_id = hashring->hash_map_id(hash_value, 0);
    addr_info->server_id_list[0] = primary_server_id;
    for(int i = 1;i < m + 1;i ++){
        addr_info->server_id_list[i] = hashring->hash_map_id(hash_value, i);
        mn_index.push_back(hashring->hash_map_index(hash_value, i));
    }
    num_mn = hashring->get_rep_num_mn(hash_value);
    hash_root_index = get_one_dimensional_index(num_mn, mn_index);
    uint64_t subtable_start_addr;
    SubTableEntryKvEcMeta *subtable_entry;
    switch(type){
        case(KV_REP):
            subtable_entry = &hashring_table->kv_ec_meta_hash_root_->subtable_entry[primary_server_id][hash_root_index];
            subtable_start_addr = hash_index_convert_40_to_64_bits(subtable_entry->subtable_entry_primary.pointer);
            addr_info->f_bucket_addr[0]  = subtable_start_addr + addr_info->f_idx * sizeof(KvEcMetaBucket);
            addr_info->f_bucket_addr_rkey[0] = server_mr_info_map_[primary_server_id]->rkey;
            break;
        case(STRIPE_META_REP):
            subtable_entry = &hashring_table->stripe_meta_hash_root_->subtable_entry[primary_server_id][hash_root_index];
            subtable_start_addr = hash_index_convert_40_to_64_bits(subtable_entry->subtable_entry_primary.pointer);
            addr_info->f_bucket_addr[0]  = subtable_start_addr + addr_info->f_idx * sizeof(RaceHashBucket);
            addr_info->s_bucket_addr[0]  = subtable_start_addr + addr_info->s_idx * sizeof(RaceHashBucket);
            addr_info->f_bucket_addr_rkey[0] = server_mr_info_map_[primary_server_id]->rkey;
            addr_info->s_bucket_addr_rkey[0] = server_mr_info_map_[primary_server_id]->rkey;
            break;
        default:
            print_mes("illegal type for get_rep_server_subtable_addr~");
            exit(0);
            break;
    }

    // replication
    for(int i = 1;i < m + 1;i ++){
        subtable_start_addr = hash_index_convert_40_to_64_bits(subtable_entry->subtable_entry_rep[i - 1].pointer);
        addr_info->f_bucket_addr[i]  = subtable_start_addr + addr_info->f_idx * sizeof(RaceHashBucket);
        addr_info->s_bucket_addr[i]  = subtable_start_addr + addr_info->s_idx * sizeof(RaceHashBucket);
        addr_info->f_bucket_addr_rkey[i] = server_mr_info_map_[addr_info->server_id_list[i]]->rkey;
        addr_info->s_bucket_addr_rkey[i] = server_mr_info_map_[addr_info->server_id_list[i]]->rkey;
    }
}

void ClientFR::get_bucket_index(KVHashInfo * hash_info, KVTableAddrInfo *addr_info){
    uint64_t hash_value = hash_info->hash_value;
    uint64_t f_index_value = subtable_index(hash_value, RACE_HASH_ADDRESSABLE_BUCKET_NUM);
    uint64_t f_idx;
    if (f_index_value % 2 == 0) 
        f_idx = f_index_value;
    else
        f_idx = f_index_value - 1;
    addr_info->f_main_idx = f_index_value % 2;
    addr_info->f_idx = f_idx;
}

void ClientFR::get_server_subtable_kv(KVHashInfo * hash_info, KVTableAddrInfo *addr_info){
    get_bucket_index(hash_info, addr_info);
    // get (m + 1)kv and kv ec meta server id;get (m + 1)subtable start addr
    get_rep_server_subtable_addr(hash_info, addr_info, KV_REP);
}

void ClientFR::get_kv_addr_info(KVHashInfo * hash_info, __OUT KVTableAddrInfo * addr_info) {
    get_server_subtable_kv(hash_info, addr_info);
}

void ClientFR::prepare_request(KVReqCtx * ctx) {
    ctx->is_finished = false;
    ctx->ret_val.ret_code = KV_OPS_SUCCESS;
    get_kv_hash_info(ctx->kv_info, &ctx->hash_info);
    get_kv_addr_info(&ctx->hash_info, &ctx->tbl_addr_info);
}

uint64_t ClientFR::connect_parity_key_to_64int(int stripeid, int parityid) {
    uint64_t result = static_cast<uint64_t>(stripeid) << 6;
    result |= static_cast<uint64_t>(parityid & 0x1F);
    return result;
}

void ClientFR::init_encoding_mm_space(){
    ectx = new EncodingCtx;
    ectx->if_encoding = false;
    ectx->kv_alloc_size = buffer->kv_alloc_size;
    ectx->block_size = buffer->block_size;
    ectx->predict_num_kv_meta = ectx->block_size / mm_->subblock_sz_;
    ectx->num_idx_rep = m + 1;
    // register encoding memeory 
    IbInfo ib_info;
    nm_->get_ib_info(&ib_info);
    uint64_t buf_size = CLIENT_PARITY_BUF + CLINET_METADATA_BUF + CLIENT_ENCODING_BUCKET_LEN;
    ectx->buf_ = mmap(NULL, buf_size, PROT_READ | PROT_WRITE, 
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    while(ectx->buf_ == NULL){
        RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " <<
            "ectx->buf_ allocate error, try again";
        sleep(1);
        ectx->buf_ = mmap(NULL, buf_size, PROT_READ | PROT_WRITE, 
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    }
    ectx->buf_mr_ = ibv_reg_mr(ib_info.ib_pd, ectx->buf_, buf_size, 
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    while(ectx->buf_mr_ == NULL){
        RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " <<
            "ectx->buf_mr_ allocate error, try again";
        sleep(1);
        ectx->buf_mr_ = ibv_reg_mr(ib_info.ib_pd, ectx->buf_, buf_size, 
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    }
    ectx->parity_buf = ectx->buf_;
    ectx->parity_chunk = new u8*[k_m];
    ectx->mm_alloc_parity_ctx = new ClientMMAllocCtx[m];
    for(int i = 0;i < k_m;i ++){
        *(ectx->parity_chunk + i) = (u8 *)((uint64_t)ectx->parity_buf + ectx->block_size * i);
    }
    ectx->rs_coding = new RsCoding(NULL, k, m, ectx->block_size);
    // initial encoding struct
    ectx->parity_info = new KVInfo[m];
    ectx->parity_hash_info = new KVHashInfo[m];
    ectx->parity_addr_info = new KVTableAddrInfo[m];
    ectx->parity_bucket_buffer = new BucketBuffer[m];
    for(int i = 0;i < m;i ++){
        ectx->parity_bucket_buffer[i].kv_modify_pr_cas_list.resize(1);
    }
    ectx->kv_meta_ctx = new KvEcMetaCtx*[k];
    for(int i = 0;i < k;i ++){
        ectx->kv_meta_ctx[i] = new KvEcMetaCtx[ectx->predict_num_kv_meta];
    }
    ectx->kv_message = new vector<KvBuffer>[k];
    ectx->en_message = new EncodingMessage[k];
    
    // init encoding space
    uint64_t ptr = (uint64_t)ectx->parity_buf + CLIENT_PARITY_BUF;
    int i;
    for(i = 0;i < m;i ++){
        init_bucket_space_big_slot(&ectx->parity_bucket_buffer[i], ptr);        
    }
    ectx->stripe_index      = new uint64_t[ectx->num_idx_rep];
    ectx->local_object_addr = new uint64_t[ectx->num_idx_rep];
    ectx->local_chunk_addr  = new uint64_t[ectx->num_idx_rep];
    for(i = 0;i < ectx->num_idx_rep;i ++){
        ectx->stripe_index[i]      = ptr;
        ptr += STR_INDEX_LEN;
        ectx->local_object_addr[i] = ptr;
        ptr += OBJ_INDEX_LEN;
        ectx->local_chunk_addr[i]  = ptr;
        ptr += CHU_INDEX_LEN;
    }
    ectx->local_bucket_addr = ptr;
    ptr += KV_BUCKET_SIZE * 2;
    ectx->slot_return_addr = ptr;
    ptr += sizeof(uint64_t);
    ectx->slot_target_addr = ptr;
    ptr += sizeof(uint64_t);

    // add pre malloc sr list
    ectx->sr_list_queue = new CircularQueue<IbvSrList *>(MAX_ENCODING_SR_LIST);
    ectx->sr_queue      = new CircularQueue<ibv_send_wr *>(MAX_ENCODING_SR_LIST);
    ectx->sge_queue     = new CircularQueue<ibv_sge *>(MAX_ENCODING_SR_LIST);
    ectx->double_sr_queue = new CircularQueue<ibv_send_wr *>(MAX_ENCODING_SR_LIST);
    ectx->double_sge_queue = new CircularQueue<ibv_sge *>(MAX_ENCODING_SR_LIST);
    IbvSrList * sr_list;
    struct ibv_send_wr * sr;
    struct ibv_sge * sge;
    for(int i = 0;i < MAX_ENCODING_SR_LIST;i ++){
        sr_list = (IbvSrList *)malloc(sizeof(IbvSrList));
        ectx->sr_list_queue->enqueue(sr_list);

        sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr));
        sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge));
        memset(sr, 0, sizeof(struct ibv_send_wr));
        memset(sge, 0, sizeof(struct ibv_sge));
        ectx->sr_queue->enqueue(sr);
        ectx->sge_queue->enqueue(sge);

        sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * 2);
        sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * 2);
        memset(sr, 0, sizeof(struct ibv_send_wr) * 2);
        memset(sge, 0, sizeof(struct ibv_sge) * 2);
        ectx->double_sr_queue->enqueue(sr);
        ectx->double_sge_queue->enqueue(sge);
    }
    ectx->num_mn = new vector<uint8_t>(k_m);
    ectx->mn_index = new vector<uint8_t>(k_m);
}

void ClientFR::free_encoding_mm_space(){
    uint64_t buf_size = CLIENT_PARITY_BUF + CLINET_METADATA_BUF + CLIENT_ENCODING_BUCKET_LEN;
    munmap(ectx->buf_, buf_size);
    ibv_dereg_mr(ectx->buf_mr_);
    delete [] ectx->parity_chunk;
    delete [] ectx->mm_alloc_parity_ctx;
    delete [] ectx->parity_info;
    delete [] ectx->parity_hash_info;
    delete [] ectx->parity_addr_info;
    delete [] ectx->parity_bucket_buffer;
    for(int i = 0;i < k;i ++){
        delete [] ectx->kv_meta_ctx[i];
    }
    delete [] ectx->kv_meta_ctx;
    delete [] ectx->kv_message;
    delete [] ectx->en_message;
}

void ClientFR::encoding_prepare_sync(){
    ectx->data_chunk_id.clear();
    ectx->parity_chunk_id.clear();
    ectx->stripe_id = pre_stripe;
    pre_stripe ++;
    EncodingMessage *enc_mes;
    int buffer_id, start_index, end_index;
    for(int i = 0;i < k;i ++){
        enc_mes     = &ectx->en_message[i];
        buffer_id   = enc_mes->buffer_id;
        start_index = enc_mes->start_index;
        end_index   = enc_mes->end_index;
        ectx->data_chunk_id.push_back(buffer_id);
        ectx->kv_message[i] = buffer->pick_data(buffer_id, start_index, end_index);
    }
    vector<int> N_num;
    for(int i = 0;i < k_m;i ++){
        N_num.push_back(i);
    }
    ectx->parity_chunk_id = difference(N_num, ectx->data_chunk_id);
    // get stripe meta ctx
    for(int i = 0;i < STRIPE_META_NUM_PID;i ++){
        if(i < m - 1){
            ectx->stripe_meta_ctx.pid[i] = ectx->parity_chunk_id[i + 1];
        } else {
            ectx->stripe_meta_ctx.pid[i] = 0;
        }   
    }
    ectx->stripe_meta_ctx.sid = ectx->stripe_id;
    ectx->stripe_meta_ctx.MF = 0;
    ectx->stripe_meta_ctx.RL = 0;
    ectx->stripe_meta_ctx.WL = 0;
}

void ClientFR::encoding_make_parity_metadata(){
    KvBuffer *kv_buffer;
    vector<KvBuffer> *kv_list;
    KvKeyLen *kvkeylen;
    KvValueLen *kvvaluelen;
    uint64_t parity_ptr;
    u8 *num_kv_size;
    int kv_align_size;
    int num_subblock;
    int bitmap_offset;
    int num_kv;
    int t;
    int kv_off;
    for(int i = 0;i < k;i ++){
        kv_list      = &ectx->kv_message[i];
        num_kv       = kv_list->size();
        parity_ptr   = (uint64_t)ectx->parity_chunk[i];
        bitmap_offset = 0;
        kv_off = 0;
        for(int j = 0;j < num_kv;j ++){
            kv_buffer = &(*kv_list)[j];
            memcpy((void *)parity_ptr, (void *)kv_buffer->kv_addr, kv_buffer->kv_len);
            kv_align_size = get_aligned_size(kv_buffer->kv_len, ectx->kv_alloc_size);
            num_subblock = kv_align_size / ectx->kv_alloc_size;
            ectx->kv_meta_ctx[i][j].sid  = ectx->stripe_id;
            ectx->kv_meta_ctx[i][j].fp   = kv_buffer->fp;
            ectx->kv_meta_ctx[i][j].cid  = i;
            ectx->kv_meta_ctx[i][j].off  = bitmap_offset;
            ectx->kv_meta_ctx[i][j].pid1 = ectx->parity_chunk_id[0];
            ectx->kv_meta_ctx[i][j].pid2 = ectx->parity_chunk_id[1];
            // record log read pointer need change
            parity_ptr += kv_align_size;
            bitmap_offset += num_subblock;
        }
    }
    ectx->rs_coding->encode_data(ectx->parity_chunk, ectx->block_size);
    mem_con.parity_data += m * (ectx->block_size + 8);
}

void ClientFR::encoding_prepare_parity(){
    int buffer_id;
    uint64_t key, hash_key;
    for(int i = 0;i < m;i ++){
        buffer_id = ectx->parity_chunk_id[i];
        key = connect_parity_key_to_64int(ectx->stripe_id, buffer_id);
        hash_key = hash_64int_to_64int(key);
        ectx->parity_info[i].l_addr = (void *)ectx->parity_chunk[k + i];
        ectx->parity_info[i].lkey = ectx->buf_mr_->lkey;
        ectx->parity_hash_info[i].hash_value = hash_key;
        hash_compute_fp(&ectx->parity_hash_info[i]);
    }
}

void ClientFR::free_sr_list(IbvSrList * sr_list, int num_sr_lists){
    for(int i = 0;i < num_sr_lists;i ++){
        free(sr_list[i].sr_list[0].sg_list);
        free(sr_list[i].sr_list);
    }
    free(sr_list);
}
 
void ClientFR::encoding_init_sr_list(){
    ectx->sr_list_batch.clear();
    ectx->sr_list_num_batch.clear();
    ectx->comp_wrid_map.clear();
}

void ClientFR::make_trace(){
    vector<KvBuffer> *kv_list;
    KvBuffer *kv_buffer;
    int pre_stripe_index = ectx->stripe_id - 1 - base_stripe_id;
    int i = 0;
    int empty_num = 0;
    while(empty_num != k){
        kv_list = &ectx->kv_message[i];
        if(kv_list->size() != 0){
            kv_buffer = &kv_list->back();
            update_trace_key_list_per_stripe[pre_stripe_index].push_back(kv_buffer->key);
            delete_trace_key_list_per_stripe[pre_stripe_index].push_back(kv_buffer->key);
            degrade_read_trace_key_list_per_stripe[pre_stripe_index].push_back(kv_buffer->key);
            kv_list->pop_back();
            empty_num = 0;
        } else {
            empty_num ++;
        }
        i = (i + 1) % k;
    }
}

void ClientFR::encoding_check(){
    if(ectx->if_encoding == false){
        int num_full = 0;
        int arr[k_m];
        for(int i = 0;i < k_m;i ++){
            if(!buffer->buffer_list[i].encoding_queue->empty()){
                num_full ++;
            }
            arr[i] = buffer->buffer_list[i].encoding_queue->size();
        }
        // need encoding
        if(num_full >= k){
            int encoding_index[k];
            top_k_indices(arr, k_m, k, encoding_index);
            for(int i = 0;i < k;i ++){
                ectx->en_message[i] = buffer->buffer_list[encoding_index[i]].encoding_queue->front();
                buffer->buffer_list[encoding_index[i]].encoding_queue->pop();
            }
            encoding_and_make_stripe_sync();
        }
    }
}

void ClientFR::encoding_check_make_trace(){
    if(ectx->if_encoding == false){
        int num_full = 0;
        int arr[k_m];
        for(int i = 0;i < k_m;i ++){
            if(!buffer->buffer_list[i].encoding_queue->empty()){
                num_full ++;
            }
            arr[i] = buffer->buffer_list[i].encoding_queue->size();
        }
        // need encoding
        if(num_full >= k){
            int encoding_index[k];
            top_k_indices(arr, k_m, k, encoding_index);
            for(int i = 0;i < k;i ++){
                ectx->en_message[i] = buffer->buffer_list[encoding_index[i]].encoding_queue->front();
                buffer->buffer_list[encoding_index[i]].encoding_queue->pop();
            }
            encoding_and_make_stripe_sync_trace();
        }
    }
}

void ClientFR::encoding_wait(){
    encoding_thread.waitDone();
}

void ClientFR::print_encoding_mes(){
    buffer->print_encoding_mes();
}

void ClientFR::print_buffer_mes(){
    buffer->print_buffer_mes();
}

void ClientFR::print_buffer_all_mes(){
    buffer->print_mes();
}

void ClientFR::init_trace(){
    // for generate update trace
    update_trace_key_list_per_stripe = new deque<string>[max_stripe];
    for(int i = 0;i < max_stripe;i ++){
        update_trace_key_list_per_stripe[i].clear();
    }
    // for generate delete trace
    delete_trace_key_list_per_stripe = new deque<string>[max_stripe];
    for(int i = 0;i < max_stripe;i ++){
        delete_trace_key_list_per_stripe[i].clear();
    }
    // for generate degrade read trace
    degrade_read_trace_key_list_per_stripe = new deque<string>[max_stripe];
    for(int i = 0;i < max_stripe;i ++){
        degrade_read_trace_key_list_per_stripe[i].clear();
    }
}

void ClientFR::encoding_finish(){
    ectx->if_encoding = false;
}

void ClientFR::encoding_finish_trace(){
    make_trace();
    ectx->if_encoding = false;
}

void ClientFR::encoding_write_stripe_index(){

    StripeIndex *stripe_index;
    uint64_t remote_addr = remote_stripe_index_addr + ectx->stripe_id * STR_INDEX_LEN;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    
    for(int i = 0;i < m + 1;i ++){
        stripe_index = (StripeIndex *)ectx->stripe_index[i];
        for(int j = 0;j < k;j ++){
            stripe_index->cid[j] = ectx->stripe_id * k + j;
        }
        for(int j = 0;j < m;j ++){
            stripe_index->pmsg[j].server_id = ectx->mm_alloc_parity_ctx[j].server_id_list;
            stripe_index->pmsg[j].addr      = ectx->mm_alloc_parity_ctx[j].addr_list;
        }
        sr_list = gen_one_sr_list((uint64_t)stripe_index, STR_INDEX_LEN, ectx->buf_mr_->lkey, 
            0, i, ENCODING_WRITE_STRIPE_INDEX, i, IBV_WR_RDMA_WRITE, remote_addr, 0, NULL);
        send_one_sr_list(sr_list, &ectx->comp_wrid_map);
    }
}

void ClientFR::encoding_write_parity_chunk(){
    encoding_prepare_parity();
    int buffer_id;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    uint8_t server_id;
    for(int i = 0;i < m;i ++){
        buffer_id = ectx->parity_chunk_id[i];
        server_id = hashring->get_ringid_num_mn(ectx->parity_hash_info[i].hash_value, 0, buffer_id);
        mm_->mm_alloc_encoding(buffer->block_size, nm_, server_id, &ectx->mm_alloc_parity_ctx[i]);
        if (ectx->mm_alloc_parity_ctx[i].addr_list < server_st_addr_ || 
            ectx->mm_alloc_parity_ctx[i].addr_list >= server_st_addr_ + server_data_len_) {
            print_error("encoding workflow:alloc parity failed~");
            exit(0);
        }
        sr_list = gen_write_kv_sr_lists_encoding(0, &ectx->parity_info[i],
            &ectx->mm_alloc_parity_ctx[i], &sr_list_num, WRITE_PARITY_CHUNK + i);
        send_one_sr_list(sr_list, &ectx->comp_wrid_map);
    }
}

void ClientFR::encoding_stripe_parity(){
    encoding_init_sr_list();
    // print_mes("debug7");
    encoding_write_parity_chunk();
    // print_mes("debug8");
    encoding_write_stripe_index();
    // print_mes("debug9");
    int ret = nm_->rdma_post_check_sr_list(&ectx->comp_wrid_map);
    if(ret == -1){
        print_error("encoding workflow:encoding stripe parity failed");
        exit(0);
    }
}

void ClientFR::encoding_read_kv_bucket_and_write_kv(KvBuffer *kv_buffer){
    uint8_t server_id = kv_buffer->kv_mes.server_id[0];
    uint64_t block_size = kv_buffer->kv_len;
    ClientMMAllocCtx mm_alloc_ctx;
    mm_->mm_alloc(block_size, nm_, server_id, &mm_alloc_ctx);
    if (mm_alloc_ctx.addr_list < server_st_addr_ || 
        mm_alloc_ctx.addr_list >= server_st_addr_ + server_data_len_) {
        print_error("encoding workflow:read_buckets_and_write_kv_sync:mm alloc failed~");
        return;
    }
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_sr_list(kv_buffer->kv_addr, block_size, input_buf_mr_->lkey, 0, server_id,
        WRITE_KV_ST_WRID, 0, IBV_WR_RDMA_WRITE, mm_alloc_ctx.addr_list, 0, NULL);
    kv_buffer->kv_addr = mm_alloc_ctx.addr_list;
    send_one_sr_list(sr_list, &ectx->comp_wrid_map);
    // ectx->sr_list_batch.push_back(sr_list);
    // ectx->sr_list_num_batch.push_back(1);
    int ret = nm_->rdma_post_check_sr_list(&ectx->comp_wrid_map);
    // ibv_wc wc;
    // int ret = nm_->rdma_post_sr_list_batch_sync(ectx->sr_list_batch, ectx->sr_list_num_batch, &wc);
    // encoding_init_sr_list();
    sr_list = gen_one_sr_list(ectx->local_bucket_addr, KV_BUCKET_SIZE * 2, ectx->buf_mr_->lkey,
        0, server_id, READ_BUCKET_ST_WRID, 0, IBV_WR_RDMA_READ, kv_buffer->kv_mes.slot_addr[1],
        0, NULL);
    send_one_sr_list(sr_list, &ectx->comp_wrid_map);
    // ectx->sr_list_batch.push_back(sr_list);
    // ectx->sr_list_num_batch.push_back(1);
    ret = nm_->rdma_post_check_sr_list(&ectx->comp_wrid_map);

}

void ClientFR::encoding_write_object_index(KvBuffer *kv_buffer, KvEcMetaCtx *kv_meta_ctx){
    uint64_t hash_value = kv_buffer->hash_key;
    int object_id = hash_value % MAX_OBJECT_INDEX;
    uint64_t remote_addr = remote_object_index_addr + object_id * OBJ_INDEX_LEN;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    ObjectIndex *object_index;
    for(int i = 0;i < ectx->num_idx_rep;i ++){
        object_index = (ObjectIndex *)ectx->local_object_addr[i];
        object_index->chunk_index = kv_meta_ctx->cid;
        object_index->length      = kv_buffer->kv_len;
        object_index->offset      = kv_meta_ctx->off;
        object_index->stripe_id   = ectx->stripe_id;
        object_index->fp          = kv_meta_ctx->fp;
        sr_list = gen_one_sr_list((uint64_t)object_index, OBJ_INDEX_LEN, ectx->buf_mr_->lkey, 0, i,
            ENCODING_WRITE_OBJECT_INDEX, i, IBV_WR_RDMA_WRITE, remote_addr, 0, NULL);
        // send_one_sr_list(sr_list, &ectx->comp_wrid_map);
        ectx->sr_list_batch.push_back(sr_list);
        ectx->sr_list_num_batch.push_back(1);
    }
}

enum{
    CHUNK_FIRST_,
    CHUNK_MIDDLE_,
    CHUNK_LAST_
};

void ClientFR::encoding_write_chunk_index(KvBuffer *kv_buffer, int obj_status, int chunk_id, 
    uint64_t *alloc_addr, uint64_t mm_alloc_addr){
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    ChunkIndex *chunk_index;
    uint64_t remote_addr;
    uint64_t hash_value = kv_buffer->hash_key;
    int object_id = hash_value % MAX_OBJECT_INDEX;
    uint64_t object_index_addr = remote_object_index_addr + object_id * OBJ_INDEX_LEN;
    for(int i = 0;i < ectx->num_idx_rep;i ++){
        chunk_index = (ChunkIndex *)ectx->local_chunk_addr[i];
        chunk_index->obj_addr = (void *)object_index_addr;
        chunk_index->pmsg.server_id = kv_buffer->kv_mes.server_id[0];
        chunk_index->pmsg.addr = mm_alloc_addr;
        if(obj_status == CHUNK_FIRST_){
            ClientMMAllocCtx mm_ctx;
            mm_->mm_alloc(CHU_INDEX_LEN, nm_, i, &mm_ctx);
            remote_addr = remote_chunk_index_addr + (ectx->stripe_id * k + chunk_id) * CHU_INDEX_LEN;
            chunk_index->next = (void *)mm_ctx.addr_list;
            alloc_addr[i] = mm_ctx.addr_list;
        } else if(obj_status == CHUNK_MIDDLE_){
            ClientMMAllocCtx mm_ctx;
            mm_->mm_alloc(CHU_INDEX_LEN, nm_, i, &mm_ctx);
            remote_addr = alloc_addr[i];
            chunk_index->next = (void *)mm_ctx.addr_list;
            alloc_addr[i] = mm_ctx.addr_list;
        } else if(obj_status == CHUNK_LAST_){
            remote_addr = alloc_addr[i];
            chunk_index->next = NULL;
        }
        sr_list = gen_one_sr_list((uint64_t)chunk_index, CHU_INDEX_LEN, ectx->buf_mr_->lkey, 0, i, 
            ENCODING_WRITE_CHUNK_INDEX, i, IBV_WR_RDMA_WRITE, remote_addr, 0, NULL);
        // send_one_sr_list(sr_list, &ectx->comp_wrid_map);
        ectx->sr_list_batch.push_back(sr_list);
        ectx->sr_list_num_batch.push_back(1);
    }
}

void ClientFR::encoding_kv_RTT1(KvBuffer *kv_buffer){
    encoding_init_sr_list();
    encoding_read_kv_bucket_and_write_kv(kv_buffer);
    // ibv_wc wc;
    // int ret = nm_->rdma_post_sr_list_batch_sync(ectx->sr_list_batch, ectx->sr_list_num_batch, &wc);
    // int ret = nm_->rdma_post_check_sr_list(&ectx->comp_wrid_map);
    // if(ret == -1){
    //     print_error("encoding workflow:encoding kv RTT1 failed");
    //     exit(0);
    // }
}

void ClientFR::encoding_ec_metadata(KvBuffer *kv_buffer, KvEcMetaCtx *kv_meta_ctx, int obj_status, 
    int chunk_id, uint64_t *alloc_addr){
    encoding_init_sr_list();
    encoding_write_object_index(kv_buffer, kv_meta_ctx);
    encoding_write_chunk_index(kv_buffer, obj_status, chunk_id, alloc_addr, kv_buffer->kv_addr);
    ibv_wc wc;
    int ret = nm_->rdma_post_sr_list_batch_sync(ectx->sr_list_batch, ectx->sr_list_num_batch, &wc);
    // int ret = nm_->rdma_post_check_sr_list(&ectx->comp_wrid_map);
    if(ret == -1){
        print_error("encoding workflow:encoding kv RTT1 failed");
        exit(0);
    }


}

void fill_race_slot(RaceHashSlot *slot, uint8_t fp, uint8_t kv_len, uint8_t server_id, uint64_t addr){
    slot->fp = fp;
    slot->kv_len = kv_len;
    slot->server_id = server_id;
    hash_index_convert_64_to_40_bits(addr, slot->pointer);
}

uint64_t find_empty_slot_get_offset(uint64_t bucket_addr){

    uint32_t f_free_num;
    uint32_t f_free_slot_idx;
    int32_t bucket_idx = -1;
    int32_t slot_idx = -1;
    int all_free_num = 0;
    for (int i = 0; i < 2; i ++) {
        if(i == 0){
            f_free_num = get_free_kv_ec_meta_slot_num((KvEcMetaBucket *)bucket_addr, 
                &f_free_slot_idx);
        }else{
            f_free_num = get_free_kv_ec_meta_slot_num((KvEcMetaBucket *)(bucket_addr + KV_BUCKET_SIZE), 
                &f_free_slot_idx);
        }
        all_free_num += f_free_num;
        if (f_free_num > 0) {
            bucket_idx = i;
            slot_idx = f_free_slot_idx;
        }
    }
    
    if(bucket_idx == -1){
        return -1;
    }

    return KV_BUCKET_SIZE * bucket_idx + sizeof(KvEcMetaSlot) * slot_idx + sizeof(uint64_t) * 2;

}

void ClientFR::encoding_cas_kv_slot(KvBuffer *kv_buffer, uint64_t *origin_value){
    RaceHashSlot *new_local_slot_ptr = (RaceHashSlot *)ectx->slot_target_addr;
    fill_race_slot(new_local_slot_ptr, kv_buffer->fp, kv_buffer->kv_len, 
        kv_buffer->kv_mes.server_id[0], kv_buffer->kv_addr);
    uint64_t sub = find_empty_slot_get_offset(ectx->local_bucket_addr);
    uint64_t local_slot_addr  = ectx->local_bucket_addr + sub;
    uint64_t remote_slot_addr = kv_buffer->kv_mes.slot_addr[1] + sub;
    *origin_value     = *(uint64_t *)local_slot_addr;
    uint8_t server_id = kv_buffer->kv_mes.server_id[0];
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_cas_sr_list(ectx->slot_return_addr, ectx->buf_mr_->lkey, 0, 
        server_id, CAS_ST_WRID, 0, remote_slot_addr, *origin_value, *(uint64_t *)new_local_slot_ptr,
        IBV_SEND_SIGNALED, NULL);
    // send_one_sr_list(sr_list, &ectx->comp_wrid_map);
    ectx->sr_list_batch.push_back(sr_list);
    ectx->sr_list_num_batch.push_back(1);
}

void ClientFR::encoding_kv_RTT2(KvBuffer *kv_buffer){
    uint64_t old_value;
    encoding_init_sr_list();
    encoding_cas_kv_slot(kv_buffer, &old_value);
    // int ret = nm_->rdma_post_check_sr_list(&ectx->comp_wrid_map);
    ibv_wc wc;
    int ret = nm_->rdma_post_sr_list_batch_sync(ectx->sr_list_batch, ectx->sr_list_num_batch, &wc);
    if(ret == -1){
        print_error("encoding workflow:encoding kv RTT2 failed");
        exit(0);
    }
    if(old_value != *(uint64_t *)ectx->slot_return_addr){
        print_error("kv insert workflow:return value is not equal to old value!");
        // exit(0);
    }
}

void ClientFR::encoding_kv(){
    vector<KvBuffer> *kv_list;
    KvBuffer *kv_buffer; 
    KvEcMetaCtx *kv_meta_ctx;
    int num_kv;
    uint64_t alloc_addr[ectx->num_idx_rep];
    for(int i = 0;i < k;i ++){
        kv_list      = &ectx->kv_message[i];
        num_kv       = kv_list->size();
        for(int j = 0;j < num_kv;j ++){
            // printf("encoding~\n");
            kv_buffer = &(*kv_list)[j];
            // RTT1
            encoding_kv_RTT1(kv_buffer);
            // RTT2
            encoding_kv_RTT2(kv_buffer);
            get_time_latency(false);
        }
    }

    for(int i = 0;i < k;i ++){
        kv_list      = &ectx->kv_message[i];
        num_kv       = kv_list->size();
        for(int j = 0;j < num_kv;j ++){
            kv_buffer = &(*kv_list)[j];
            kv_meta_ctx = &ectx->kv_meta_ctx[i][j];
            if(j == 0){
                encoding_ec_metadata(kv_buffer, kv_meta_ctx, CHUNK_FIRST_,  i, alloc_addr);
            }else if(j == num_kv - 1){
                encoding_ec_metadata(kv_buffer, kv_meta_ctx, CHUNK_LAST_,   i, alloc_addr);
            }else {
                encoding_ec_metadata(kv_buffer, kv_meta_ctx, CHUNK_MIDDLE_, i, alloc_addr);
            }
        }
    }
}

void ClientFR::encoding_and_make_stripe_sync(){
    
    // print_mes("debug1");
    encoding_prepare_sync();
    // print_mes("debug2");
    encoding_make_parity_metadata();
    // print_mes("debug3");

    // write stripe Index and Parity chunk
    encoding_stripe_parity();
    // print_mes("debug4");

    /*
        for all kv 
            1. read bucket, write kv, write object Index, write chunk Index
            2. cas kv slot
    
    */
    encoding_kv();
    // print_mes("debug5");
    encoding_finish();
}

void ClientFR::encoding_and_make_stripe_sync_trace(){
    
    // print_mes("debug1");
    encoding_prepare_sync();
    // print_mes("debug2");
    encoding_make_parity_metadata();
    // print_mes("debug3");

    // write stripe Index and Parity chunk
    encoding_stripe_parity();
    // print_mes("debug4");

    /*
        for all kv 
            1. read bucket, write kv, write object Index, write chunk Index
            2. cas kv slot
    
    */
    encoding_kv();
    // print_mes("debug5");
    encoding_finish_trace();
}

void ClientFR::kv_insert_buffer(KVReqCtx * ctx){
    KvEcMetaMes kv_mes;
    uint64_t offset;
    offset = ctx->bucket_idx * sizeof(RaceHashBucket) + sizeof(uint64_t) + ctx->slot_idx * sizeof(RaceHashSlot);
    
    kv_mes.server_id[0] = ctx->tbl_addr_info.server_id_list[0];
    kv_mes.slot_addr[0] = ctx->tbl_addr_info.f_bucket_addr[0] + offset * 2 + sizeof(RaceHashSlot);
    kv_mes.slot_addr[1] = ctx->tbl_addr_info.f_bucket_addr[0];

    buffer->insert(ctx->hash_info.hash_value % k_m, ctx->key_str,
        (uint64_t)ctx->kv_info->l_addr,
        ctx->kv_all_len, ctx->hash_info.hash_value, kv_mes, ctx->hash_info.fp);
}

int ClientFR::kv_insert_sync(KVReqCtx * ctx) {
    // print_mes("debug6"); 
    prepare_request(ctx);
    // kv_insert_read_buckets_and_write_kv(ctx);
    // if (ctx->is_finished) {
    //     return ctx->ret_val.ret_code;
    // }
    // kv_insert_write_buckets(ctx);
    // if(ctx->is_finished){
    //     return ctx->ret_val.ret_code;
    // }
    kv_insert_buffer(ctx);
    // print_mes("debug1");
    return 0;
}

void ClientFR::kv_search_read_buckets_sync(KVReqCtx * ctx) {
    int ret = 0;
    ctx->comp_wrid_map.clear();
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, ctx->tbl_addr_info.server_id_list[0], READ_BUCKET_ST_WRID, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("search workflow:read_buckets send sr list failed~");
        exit(0);
    }
    ctx->is_finished = false;
    return;
}

void ClientFR::get_local_bucket_info_kv(KVReqCtx * ctx) {
    ctx->f_com_bucket = ctx->local_bucket_addr;
    ctx->bucket_arr[0] = ctx->f_com_bucket;
    ctx->bucket_arr[1] = ctx->f_com_bucket + 2;
    ctx->slot_arr[0] = ctx->f_com_bucket[0].slots;
    ctx->slot_arr[1] = ctx->f_com_bucket[1].slots;
}

bool ClientFR::find_kv_slot_in_buckets(KvEcMetaBucket *bucket, KVRWAddr *rw, 
    pair<int32_t, int32_t> *pair, uint8_t fp, uint64_t local_addr, int op_type){
    uint64_t pre_addr = local_addr;
    bool ret = false;
    RaceHashSlot *slot;
    EcMetaSlot *ec_slot;
    uint8_t kv_len;
    uint8_t server_id;
    bool check;
    for(int i = 0;i < 2;i ++){
        for(int j = 0;j < RACE_HASH_ASSOC_NUM;j ++){
            slot = &bucket[i].slots[j].kv_slot;
            ec_slot = &bucket[i].slots[j].ec_meta_slot;
            kv_len = get_kv_slot_len(slot);
            if(op_type == KV_REQ_SEARCH){
                check = (slot->fp == fp && kv_len != 0);
            } else if(op_type == KV_REQ_UPDATE || op_type == KV_REQ_DELETE){
                check = (slot->fp == fp && kv_len != 0 && *(uint64_t *)ec_slot != 0);
            }
            if(check){
                server_id = get_kv_slot_server(slot);
                rw->l_kv_addr = local_addr;
                rw->length = kv_len * mm_->subblock_sz_;
                rw->lkey = local_buf_mr_->lkey;
                rw->r_kv_addr = hash_index_convert_40_to_64_bits(slot->pointer);
                rw->rkey = server_mr_info_map_[server_id]->rkey;
                rw->server_id = server_id;
                pair->first = i;
                pair->second = j;
                ret = true;
                break;
            }
        }
        if(ret == true){
            break;
        }
    }
    return ret;
}

void ClientFR::find_kv_slot(KVReqCtx * ctx) {
    get_local_bucket_info_kv(ctx);
    uint64_t local_kv_buf_addr = (uint64_t)ctx->local_kv_addr;
    ctx->kv_read_addr_list.clear();
    ctx->kv_idx_list.clear();
    bool if_find;
    KVRWAddr rw;
    pair<int32_t, int32_t> my_pair;
    KvEcMetaBucket *bucket = (KvEcMetaBucket *)ctx->local_bucket_addr;
    if_find = find_kv_slot_in_buckets(bucket,
        &rw, &my_pair, ctx->hash_info.fp, local_kv_buf_addr, ctx->req_type);
    if(if_find == false) {
        // print_error("find kv in bucket failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        // code_num_count.cache_hit ++;
    } else {
        ctx->kv_read_addr_list.push_back(rw);
        ctx->kv_idx_list.push_back(my_pair);
        ctx->bucket_idx = ctx->kv_idx_list[0].first;
        ctx->slot_idx   = ctx->kv_idx_list[0].second;
    }
}

void ClientFR::kv_search_read_kv_sync(KVReqCtx * ctx) {
    int ret = 0;
    find_kv_slot(ctx);
    ctx->comp_wrid_map.clear();
    if(ctx->kv_read_addr_list.size() == 0){
        // print_error("search workflow:read kv:find kv in buckets failed~");
        ctx->ret_val.value_addr = NULL;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &sr_list_num);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_mes("search workflow:read_kv send sr list failed~");
    }
    ctx->is_finished = false;
    return;
}

int32_t ClientFR::find_match_kv_idx(KVReqCtx * ctx) {
    int32_t ret = 0;
    for (size_t i = 0; i < ctx->kv_read_addr_list.size(); i ++) {
        uint64_t read_key_addr = ctx->kv_read_addr_list[i].l_kv_addr + sizeof(KvKeyLen) + sizeof(KvValueLen);
        uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KvKeyLen) + sizeof(KvValueLen);
        KvKeyLen * header = (KvKeyLen *)ctx->kv_read_addr_list[i].l_kv_addr;
        if (check_key((void *)read_key_addr, char_to_int((u8 *)ctx->kv_read_addr_list[i].l_kv_addr, 
            sizeof(KvKeyLen)), (void *)local_key_addr, ctx->kv_info->key_len)) {
            return i;
        }
    }
    return -1;
}

void ClientFR::kv_search_check_kv(KVReqCtx * ctx) {
    int32_t match_idx = find_match_kv_idx(ctx);
    if (match_idx != -1) { 
        uint64_t read_key_addr = ctx->kv_read_addr_list[match_idx].l_kv_addr + sizeof(KvKeyLen) + sizeof(KvValueLen);
        KvKeyLen * header = (KvKeyLen *)ctx->kv_read_addr_list[match_idx].l_kv_addr; 
        ctx->ret_val.value_addr = (void *)(read_key_addr + char_to_int((u8 *)header, sizeof(KvKeyLen)));
        ctx->is_finished = true;
        return;
    } else {
        // print_error("search workflow: no match kv~");
        ctx->ret_val.value_addr = NULL;
        ctx->is_finished = true;
        return;
    }
}

void *ClientFR::kv_search_sync(KVReqCtx * ctx) { 
    prepare_request(ctx);
    kv_search_read_buckets_sync(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.value_addr;
    }
    kv_search_read_kv_sync(ctx); 
    kv_search_check_kv(ctx);
    return ctx->ret_val.value_addr;
}

void ClientFR::kv_search_read_buckets_async(KVReqCtx * ctx) {
    int ret = 0;
    ctx->comp_wrid_map.clear();
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, ctx->tbl_addr_info.server_id_list[0], READ_BUCKET_ST_WRID, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("search workflow:read_buckets send sr list failed~");
        exit(0);
    }
    ctx->is_finished = false;
    return;
}

void ClientFR::kv_search_read_kv_async(KVReqCtx * ctx) {
    int ret = 0;
    find_kv_slot(ctx);
    ctx->comp_wrid_map.clear();
    if(ctx->kv_read_addr_list.size() == 0){
        // print_error("search workflow:read kv:find kv in buckets failed~");
        ctx->ret_val.value_addr = NULL;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &sr_list_num);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_mes("search workflow:read_kv send sr list failed~");
    }
    ctx->is_finished = false;
    return;
}

void *ClientFR::kv_search(KVReqCtx *ctx) {
    prepare_request(ctx);
    kv_search_read_buckets_async(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.value_addr;
    }
    kv_search_read_kv_async(ctx);
    kv_search_check_kv(ctx);
    return ctx->ret_val.value_addr;
}

void init_req_comp_wrid_map(KVReqCtx *ctx){
    ctx->comp_wrid_map.clear();
    ctx->sr_list_batch.clear();
    ctx->sr_list_num_batch.clear();
}

void ClientFR::kv_update_read_bucket_and_write_kv(KVReqCtx *ctx){
    uint32_t kv_block_size = ctx->kv_all_len;
    uint64_t server_id = ctx->tbl_addr_info.server_id_list[0];
    mm_->mm_alloc(kv_block_size, nm_, server_id, &ctx->mm_alloc_ctx);
    if(ctx->mm_alloc_ctx.addr_list == 0){
        print_mes("mm alloc failed~");
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    if (ctx->mm_alloc_ctx.addr_list < server_st_addr_ || ctx->mm_alloc_ctx.addr_list >= 
        server_st_addr_ + server_data_len_) {
        print_error("update workflow:RTT1 mm alloc failed to write kv~");
        exit(0);
    }
    // prepare write kv and read bucket sr lists
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, server_id, READ_KV_BUCKET, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ctx->sr_list_batch.push_back(sr_list);
    ctx->sr_list_num_batch.push_back(1);
    // req_append_sr_list(ctx, sr_list);
    sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &sr_list_num, WRITE_KV_ST_WRID);
    // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ctx->sr_list_batch.push_back(sr_list);
    ctx->sr_list_num_batch.push_back(1);
    // req_append_sr_list(ctx, sr_list);
}

void ClientFR::kv_update_read_object_index(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    uint64_t remote_addr = remote_object_index_addr + OBJ_INDEX_LEN * (ctx->hash_info.hash_value % MAX_OBJECT_INDEX);
    sr_list = gen_one_sr_list(uctx->object_index_addr, OBJ_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id, 
        0, UPDATE_READ_OBJECT_INDEX, 0, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
    // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ctx->sr_list_batch.push_back(sr_list);
    ctx->sr_list_num_batch.push_back(1);
    // req_append_sr_list(ctx, sr_list);
}

void ClientFR::kv_update_RTT1_sync(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_update_read_bucket_and_write_kv(ctx);
    kv_update_read_object_index(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    // int ret = req_batch_send_and_wait(ctx);
    if(ret == -1){
        print_error("kv update:RTT1 send failed~");
        exit(0);
    }
}

void ClientFR::kv_update_read_kv(KVReqCtx *ctx){
    int ret = 0;
    find_kv_slot(ctx);
    // ctx->comp_wrid_map.clear();
    if(ctx->kv_read_addr_list.size() == 0){
        // print_error("search workflow:read kv:find kv in buckets failed~");
        ctx->ret_val.value_addr = NULL;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &sr_list_num);
    // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    // nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    // if(ret == -1){
    //     print_mes("update workflow:read_kv send sr list failed~");
    // }
    ctx->sr_list_batch.push_back(sr_list);
    ctx->sr_list_num_batch.push_back(1);
    ctx->is_finished = false;
    return;
}

void ClientFR::kv_update_read_stripe_index(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    ObjectIndex *object_index = (ObjectIndex *)uctx->object_index_addr;
    if(object_index->fp != ctx->hash_info.fp){
        // print_error("kv update: find object index error: fp not equal~");
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    uctx->stripe_id = object_index->stripe_id;
    uint64_t remote_addr = remote_stripe_index_addr + STR_INDEX_LEN * object_index->stripe_id;

    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_sr_list(uctx->stripe_index_addr, STR_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id, 
        0, UPDATE_READ_STRIPE_INDEX, 0, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
    // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ctx->sr_list_batch.push_back(sr_list);
    ctx->sr_list_num_batch.push_back(1);
}

void ClientFR::kv_update_RTT2_sync(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_update_read_kv(ctx);
    kv_update_read_stripe_index(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        return;
    }
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv update:RTT2 send failed~");
        exit(0);
    }
}

void ClientFR::kv_update_read_parity_chunk(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    uint64_t remote_addr;
    uint8_t server_id;
    StripeIndex *stripe_index = (StripeIndex *)uctx->stripe_index_addr;
    ObjectIndex *object_index = (ObjectIndex *)uctx->object_index_addr;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    for(int i = 0;i < m;i ++){
        server_id   = stripe_index->pmsg[i].server_id;
        remote_addr = stripe_index->pmsg[i].addr + object_index->offset * mm_->subblock_sz_;
        sr_list = gen_one_sr_list(uctx->parity_chunk_addr[i], mm_->subblock_sz_, local_buf_mr_->lkey, ctx->coro_id,
            server_id, UPDATE_READ_PARITY_CHUNK, i, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
        // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
        ctx->sr_list_batch.push_back(sr_list);
        ctx->sr_list_num_batch.push_back(1);      
    }
}  

void ClientFR::kv_update_RTT3_sync(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_update_read_parity_chunk(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv update:RTT3 send failed~");
        exit(0);
    }
}

void ClientFR::kv_update_encoding_parity(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    ObjectIndex *object_index = (ObjectIndex *)uctx->object_index_addr;
    u8 *kv_chunk = (u8 *)ctx->kv_info->l_addr;
    u8 **parity_chunk;
    parity_chunk = new u8*[m];
    for(int i = 0;i < m;i ++){
        parity_chunk[i] = (u8 *)(uctx->parity_chunk_addr[i]);
    }
    ec_encode_data(ctx->kv_all_len, 1, m, ectx->rs_coding->g_tbls_encoding, &kv_chunk, parity_chunk);
    delete [] parity_chunk;
}


void ClientFR::kv_update_write_parity(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    StripeIndex *stripe_index = (StripeIndex *)uctx->stripe_index_addr;
    ObjectIndex *object_index = (ObjectIndex *)uctx->object_index_addr;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    ClientMMAllocCtx mm_alloc_ctx;
    uint8_t server_id;
    uint64_t remote_addr;
    for(int i = 0;i < m;i ++){
        server_id = stripe_index->pmsg[i].server_id;
        remote_addr = stripe_index->pmsg[i].addr + object_index->offset * mm_->subblock_sz_;
        sr_list = gen_one_sr_list(uctx->parity_chunk_addr[i], mm_->subblock_sz_, local_buf_mr_->lkey, ctx->coro_id,
            server_id, UPDATE_WRITE_PARITY_CHUNK, i, IBV_WR_RDMA_WRITE, remote_addr, 0, NULL);
        // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
        ctx->sr_list_batch.push_back(sr_list);
        ctx->sr_list_num_batch.push_back(1);
    }
}

void ClientFR::kv_update_write_stripe_index_chain_walking(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    uint64_t remote_addr = remote_stripe_index_addr + STR_INDEX_LEN * uctx->stripe_id;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    for(int i = 0;i < ectx->num_idx_rep;i ++){
        sr_list = gen_one_sr_list(uctx->stripe_index_addr, STR_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id,
            i, UPDATE_WRITE_STRIPE_INDEX, i, IBV_WR_RDMA_WRITE, remote_addr, 0 ,NULL);
        // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
        ctx->sr_list_batch.push_back(sr_list);
        ctx->sr_list_num_batch.push_back(1);
    }

    // simulate chunk chain walking
    ObjectIndex *object_index = (ObjectIndex *)uctx->object_index_addr;

    for(int i = 0;i < object_index->offset;i ++){
        sr_list = gen_one_sr_list(uctx->stripe_index_addr, STR_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id,
            0, UPDATE_CHAIN_WALKING, i, IBV_WR_RDMA_WRITE, remote_addr, 0 ,NULL);
        // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
        ctx->sr_list_batch.push_back(sr_list);
        ctx->sr_list_num_batch.push_back(1);
    }

}

void ClientFR::kv_update_RTT4_sync(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_update_encoding_parity(ctx);
    kv_update_write_parity(ctx);
    kv_update_write_stripe_index_chain_walking(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv update:RTT4 send failed~");
        exit(0);
    }
}

void ClientFR::fill_slot(ClientMMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info,
    __OUT RaceHashSlot * local_slot) {
    local_slot->fp = a_kv_hash_info->fp;
    local_slot->kv_len = mm_alloc_ctx->num_subblocks;
    local_slot->server_id = mm_alloc_ctx->server_id_list;
    hash_index_convert_64_to_40_bits(mm_alloc_ctx->addr_list, local_slot->pointer);
}

void ClientFR::kv_update_cas_slot(KVReqCtx *ctx){
    int32_t bucket_idx = ctx->bucket_idx;
    int32_t slot_idx   = ctx->slot_idx;
    if(bucket_idx < 0 || slot_idx > RACE_HASH_ASSOC_NUM){
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    uint64_t sub = KV_BUCKET_SIZE * bucket_idx + sizeof(KvEcMetaSlot) * slot_idx + sizeof(uint64_t) * 2;
    uint64_t remote_slot_addr = ctx->tbl_addr_info.f_bucket_addr[0] + sub; 
    RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
    fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
    uint64_t new_value = *(uint64_t *)new_local_slot_ptr;
    RaceHashSlot * old_local_slot_ptr = (RaceHashSlot *)((uint64_t)ctx->local_bucket_addr + sub);
    uint64_t old_value = *(uint64_t *)old_local_slot_ptr;
    ctx->old_value = old_value;
    IbvSrList *sr_list;
    sr_list = gen_one_cas_sr_list((uint64_t)ctx->local_cas_return_value_addr, local_buf_mr_->lkey, ctx->coro_id, 
        ctx->tbl_addr_info.server_id_list[0], CAS_ST_WRID, 0, remote_slot_addr, old_value, new_value,
        0, NULL);
    // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ctx->sr_list_batch.push_back(sr_list);
    ctx->sr_list_num_batch.push_back(1);
}

void ClientFR::kv_update_RTT5_sync(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_update_cas_slot(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv update:RTT5 send failed~");
        exit(0);
    }
    if(ctx->old_value != *(uint64_t *)ctx->local_cas_return_value_addr){
        print_error("kv update:RTT5 cas error:return value is not equal to old value~");
        exit(0);
    }
    ctx->ret_val.ret_code = KV_OPS_SUCCESS;
}

int ClientFR::kv_update_sync(KVReqCtx * ctx){
    kv_update_RTT1_sync(ctx);
    kv_update_RTT2_sync(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        return ctx->ret_val.ret_code;
    }
    kv_update_RTT3_sync(ctx);
    kv_update_RTT4_sync(ctx);
    kv_update_RTT5_sync(ctx);
    return ctx->ret_val.ret_code;
}

void ClientFR::kv_update_RTT1_async(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_update_read_bucket_and_write_kv(ctx);
    if(ctx->mm_alloc_ctx.addr_list == 0){
        return;
    }
    kv_update_read_object_index(ctx);
    // int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync_send(ctx->sr_list_batch, 
        ctx->sr_list_num_batch, ctx->comp_wrid_map);
    int ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv update:RTT1 send failed~");
        exit(0);
    }
}

void ClientFR::kv_update_RTT2_async(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_update_read_kv(ctx);
    kv_update_read_stripe_index(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        // print_mes("read stripe index failed~");
        return;
    }
    // int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    nm_->rdma_post_sr_list_batch_sync_send(ctx->sr_list_batch, ctx->sr_list_num_batch,
        ctx->comp_wrid_map);
    int ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv update:RTT2 send failed~");
        exit(0);
    }
}

void ClientFR::kv_update_RTT3_async(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_update_read_parity_chunk(ctx);
    // int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    nm_->rdma_post_sr_list_batch_sync_send(ctx->sr_list_batch, ctx->sr_list_num_batch,
        ctx->comp_wrid_map);
    int ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv update:RTT3 send failed~");
        exit(0);
    }
}

void ClientFR::kv_update_RTT4_async(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_update_encoding_parity(ctx);
    kv_update_write_parity(ctx);
    kv_update_write_stripe_index_chain_walking(ctx);
    // int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    nm_->rdma_post_sr_list_batch_sync_send(ctx->sr_list_batch, ctx->sr_list_num_batch,
        ctx->comp_wrid_map);
    int ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv update:RTT4 send failed~");
        exit(0);
    }
}

void ClientFR::kv_update_RTT5_async(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_update_cas_slot(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        return;
    }
    // int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    nm_->rdma_post_sr_list_batch_sync_send(ctx->sr_list_batch, ctx->sr_list_num_batch,
        ctx->comp_wrid_map);
    int ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv update:RTT5 send failed~");
        exit(0);
    }
    if(ctx->old_value != *(uint64_t *)ctx->local_cas_return_value_addr){
        // print_error("kv update:RTT5 cas error:return value is not equal to old value~");
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
        // exit(0);
    }
    ctx->ret_val.ret_code = KV_OPS_SUCCESS;
}

int ClientFR::kv_update_async(KVReqCtx * ctx){
    kv_update_RTT1_async(ctx);
    if(ctx->mm_alloc_ctx.addr_list == 0){
        return ctx->ret_val.ret_code;
    }
    kv_update_RTT2_async(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        return ctx->ret_val.ret_code;
    }
    kv_update_RTT3_async(ctx);
    kv_update_RTT4_async(ctx);
    kv_update_RTT5_async(ctx);
    return ctx->ret_val.ret_code;
}

void ClientFR::kv_delete_read_bucket(KVReqCtx *ctx){
    uint64_t server_id = ctx->tbl_addr_info.server_id_list[0];
    // prepare write kv and read bucket sr lists
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, server_id, READ_KV_BUCKET, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
}

void ClientFR::kv_delete_read_object_index(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    uint64_t remote_addr = remote_object_index_addr + OBJ_INDEX_LEN * (ctx->hash_info.hash_value % MAX_OBJECT_INDEX);
    sr_list = gen_one_sr_list(dctx->object_index_addr, OBJ_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id, 
        0, DELETE_READ_OBJECT_INDEX, 0, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
}

void ClientFR::kv_delete_RTT1_sync(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_delete_read_bucket(ctx);
    kv_delete_read_object_index(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv delete:RTT1 send failed~");
        exit(0);
    }
}

void ClientFR::kv_delete_read_kv(KVReqCtx *ctx){
    int ret = 0;
    find_kv_slot(ctx);
    ctx->comp_wrid_map.clear();
    if(ctx->kv_read_addr_list.size() == 0){
        // print_error("search workflow:read kv:find kv in buckets failed~");
        ctx->ret_val.value_addr = NULL;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &sr_list_num);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_mes("delete workflow:read_kv send sr list failed~");
    }
    ctx->is_finished = false;
    return;
}

void ClientFR::kv_delete_read_stripe_index(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    ObjectIndex *object_index = (ObjectIndex *)dctx->object_index_addr;
    if(object_index->fp != ctx->hash_info.fp){
        // print_error("kv update: find object index error: fp not equal~");
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    dctx->stripe_id = object_index->stripe_id;
    uint64_t remote_addr = remote_stripe_index_addr + STR_INDEX_LEN * object_index->stripe_id;

    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_sr_list(dctx->stripe_index_addr, STR_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id, 
        0, DELETE_READ_STRIPE_INDEX, 0, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
}

void ClientFR::kv_delete_remove_object_index(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    *(uint64_t *)dctx->object_0_addr = 0;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    uint64_t remote_addr = remote_object_index_addr + OBJ_INDEX_LEN * (ctx->hash_info.hash_value % MAX_OBJECT_INDEX);
    for(int i = 0;i < ectx->num_idx_rep;i ++){
        sr_list = gen_one_sr_list(dctx->object_0_addr, OBJ_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id,
            i, DELETE_REMOVE_OBJECT_INDEX, i, IBV_WR_RDMA_WRITE, remote_addr, 0, NULL);
        send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    }
}

void ClientFR::kv_delete_RTT2_sync(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_delete_read_kv(ctx);
    kv_delete_read_stripe_index(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        return;
    }
    kv_delete_remove_object_index(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv delete:RTT2 send failed~");
        exit(0);
    }
}

void ClientFR::kv_delete_read_parity_chunk(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    uint64_t remote_addr;
    uint8_t server_id;
    StripeIndex *stripe_index = (StripeIndex *)dctx->stripe_index_addr;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    for(int i = 0;i < m;i ++){
        server_id   = stripe_index->pmsg[i].server_id;
        remote_addr = stripe_index->pmsg[i].addr;
        sr_list = gen_one_sr_list(dctx->parity_chunk_addr[i], buffer->block_size, local_buf_mr_->lkey, ctx->coro_id,
            server_id, UPDATE_READ_PARITY_CHUNK, i, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
        send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    }
}

void ClientFR::kv_delete_RTT3_sync(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_delete_read_parity_chunk(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv delete:RTT3 send failed~");
        exit(0);
    }
}

void ClientFR::kv_delete_remove_parity(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    ObjectIndex *object_index = (ObjectIndex *)dctx->object_index_addr;
    for(int i = 0;i < m;i ++){
        memset((void *)(dctx->parity_chunk_addr[i] + mm_->subblock_sz_ * (object_index->offset)), 0,
            ctx->kv_all_len);
    }
}

void ClientFR::kv_delete_write_parity(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    StripeIndex *stripe_index = (StripeIndex *)dctx->stripe_index_addr;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    ClientMMAllocCtx mm_alloc_ctx;
    uint8_t server_id;
    uint64_t remote_addr;
    for(int i = 0;i < m;i ++){
        server_id = stripe_index->pmsg[i].server_id;
        mm_->mm_alloc(buffer->block_size, nm_, server_id, &mm_alloc_ctx);
        remote_addr = mm_alloc_ctx.addr_list;
        stripe_index->pmsg[i].addr = remote_addr;
        sr_list = gen_one_sr_list(dctx->parity_chunk_addr[i], buffer->block_size, local_buf_mr_->lkey, ctx->coro_id,
            server_id, UPDATE_WRITE_PARITY_CHUNK, i, IBV_WR_RDMA_WRITE, remote_addr, 0, NULL);
        send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    }
}

void ClientFR::kv_delete_write_stripe_index(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    uint64_t remote_addr = remote_stripe_index_addr + STR_INDEX_LEN * dctx->stripe_id;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    for(int i = 0;i < ectx->num_idx_rep;i ++){
        sr_list = gen_one_sr_list(dctx->stripe_index_addr, STR_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id,
            i, UPDATE_WRITE_STRIPE_INDEX, i, IBV_WR_RDMA_WRITE, remote_addr, 0 ,NULL);
        send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    }
}

void ClientFR::kv_delete_RTT4_sync(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_delete_remove_parity(ctx);
    kv_delete_write_parity(ctx);
    kv_delete_write_stripe_index(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv delete:RTT4 send failed~");
        exit(0);
    }
}

void ClientFR::kv_delete_cas_slot(KVReqCtx *ctx){
    int32_t bucket_idx = ctx->bucket_idx;
    int32_t slot_idx   = ctx->slot_idx;
    uint64_t sub = KV_BUCKET_SIZE * bucket_idx + sizeof(KvEcMetaSlot) * slot_idx + sizeof(uint64_t) * 2;
    uint64_t remote_slot_addr = ctx->tbl_addr_info.f_bucket_addr[0] + sub; 
    // uint64_t new_value = 0;
    RaceHashSlot * old_local_slot_ptr = (RaceHashSlot *)((uint64_t)ctx->local_bucket_addr + sub);
    uint64_t old_value = *(uint64_t *)old_local_slot_ptr;
    // for test
    uint64_t new_value = old_value;
    ctx->old_value = old_value;
    IbvSrList *sr_list;
    sr_list = gen_one_cas_sr_list((uint64_t)ctx->local_cas_return_value_addr, local_buf_mr_->lkey, ctx->coro_id, 
        ctx->tbl_addr_info.server_id_list[0], CAS_ST_WRID, 0, remote_slot_addr, old_value, new_value,
        0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
}

void ClientFR::kv_delete_RTT5_sync(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_delete_cas_slot(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv delete:RTT5 send failed~");
        exit(0);
    }
    if(ctx->old_value != *(uint64_t *)ctx->local_cas_return_value_addr){
        print_error("kv delete:RTT5 cas error:return value is not equal to old value~");
        exit(0);
    }
    ctx->ret_val.ret_code = KV_OPS_SUCCESS;
}

int ClientFR::kv_delete_sync(KVReqCtx * ctx){
    kv_delete_RTT1_sync(ctx);
    kv_delete_RTT2_sync(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        return ctx->ret_val.ret_code;
    }
    kv_delete_RTT3_sync(ctx);
    kv_delete_RTT4_sync(ctx);
    kv_delete_RTT5_sync(ctx);
    return ctx->ret_val.ret_code;
}

void ClientFR::kv_degrade_read_object_index(KVReqCtx *ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    uint64_t remote_addr = remote_object_index_addr + OBJ_INDEX_LEN * (ctx->hash_info.hash_value % MAX_OBJECT_INDEX);
    sr_list = gen_one_sr_list(drctx->object_index_addr, OBJ_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id, 
        0, DEGRADE_READ_OBJECT_INDEX, 0, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
}

void ClientFR::kv_degrade_RTT1(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_degrade_read_object_index(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv degrade read:RTT1 send failed~");
        exit(0);
    }
}

void ClientFR::kv_degrade_read_stripe_index(KVReqCtx *ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    ObjectIndex *object_index = (ObjectIndex *)drctx->object_index_addr;
    if(object_index->fp != ctx->hash_info.fp){
        // print_error("kv update: find object index error: fp not equal~");
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    drctx->stripe_id = object_index->stripe_id;
    uint64_t remote_addr = remote_stripe_index_addr + STR_INDEX_LEN * object_index->stripe_id;

    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_sr_list(drctx->stripe_index_addr, STR_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id, 
        0, DEGRADE_READ_STRIPE_INDEX, 0, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
}

void ClientFR::kv_degrade_RTT2(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_degrade_read_stripe_index(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        return;
    }
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv degrade read:RTT2 send failed~");
        exit(0);
    }
}

void ClientFR::kv_degrade_read_parity_chunk(KVReqCtx *ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    uint64_t remote_addr;
    uint8_t server_id;
    StripeIndex *stripe_index = (StripeIndex *)drctx->stripe_index_addr;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    // cout << "stripe: " << drctx->stripe_id << endl;
    // if(drctx->stripe_id == 0){
    //     cout << "num ctx: " << ctx->ctx_id << endl;
    // }
    server_id   = stripe_index->pmsg[0].server_id;
    remote_addr = stripe_index->pmsg[0].addr;
    sr_list = gen_one_sr_list(drctx->parity_chunk_addr[0], buffer->block_size, local_buf_mr_->lkey, ctx->coro_id,
        server_id, DEGRADE_READ_PARITY_CHUNK, 0, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
}

void ClientFR::kv_degrade_RTT3(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_degrade_read_parity_chunk(ctx);
    int ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv degrade read:RTT3 send failed~");
        exit(0);
    }
}

void ClientFR::kv_degrade_get_kv_chain_walking(KVReqCtx *ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    StripeIndex *stripe_index = (StripeIndex *)drctx->stripe_index_addr;
    ObjectIndex *object_index = (ObjectIndex *)drctx->object_index_addr;
    uint64_t remote_addr;
    ChunkIndex *chunk_index;
    int ret;
    IbvSrList *sr_list;
    for(int i = 0;i < object_index->offset + 1;i ++){
        init_req_comp_wrid_map(ctx);
        for(int j = 0;j < k;j ++){
            if(object_index->chunk_index == j){
                continue;
            }
            if(i == 0){
                remote_addr = remote_chunk_index_addr + CHU_INDEX_LEN * stripe_index->cid[j];
                sr_list = gen_one_sr_list(drctx->related_kv_addr[j], CHU_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id,
                    0, DEGRADE_READ_CHUNK_INDEX, j, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
                send_one_sr_list(sr_list, &ctx->comp_wrid_map);
            } else {
                chunk_index = (ChunkIndex *)drctx->related_kv_addr[j];
                remote_addr = (uint64_t)chunk_index->next;
                sr_list = gen_one_sr_list(drctx->related_kv_addr[j], CHU_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id,
                    0, DEGRADE_READ_CHUNK_INDEX, j, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
                send_one_sr_list(sr_list, &ctx->comp_wrid_map);
            }
        }
        ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
        if(ret == -1){
            print_error("kv degrade read:RTT4 chain walking failed~");
            exit(0);
        }
    }
    uint8_t server_id;
    init_req_comp_wrid_map(ctx);
    for(int j = 0;j < k;j ++){
        if(object_index->chunk_index == j){
            continue;
        }
        chunk_index = (ChunkIndex *)drctx->related_kv_addr[j];
        server_id   = chunk_index->pmsg.server_id;
        remote_addr = chunk_index->pmsg.addr;
        sr_list = gen_one_sr_list(drctx->related_kv_addr[j], CHU_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id,
            server_id, DEGRADE_READ_RELATED_KV, j, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
        send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    }
    ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv degrade read:RTT4 read related kv failed~");
        exit(0);
    }
}

void ClientFR::kv_degrade_RTT4(KVReqCtx *ctx){
    kv_degrade_get_kv_chain_walking(ctx);
}

void ClientFR::kv_degrade_decoding(KVReqCtx *ctx){

    DegradeReadCtx *drctx = ctx->drctx;
    ObjectIndex *object_index = (ObjectIndex *)drctx->object_index_addr;

    u8 **data = drctx->data;

    int count = 0;
    for(int i = 0;i < k;i ++){
        if(i == object_index->chunk_index){
            continue;
        }
        data[count] = (u8 *)drctx->related_kv_addr[i];
        count ++;
    }
    data[k - 1] = (u8 *)(drctx->parity_chunk_addr[0] + object_index->offset * mm_->subblock_sz_);
    data[k] = (u8 *)drctx->related_kv_addr[object_index->chunk_index];

    u8 frag_error[1];
    frag_error[0] = object_index->chunk_index;
    ectx->rs_coding->decode_data(data, mm_->subblock_sz_, frag_error, 1);
    KvValueLen *value_len_addr = (KvValueLen *)((uint64_t)data[k] + sizeof(KvKeyLen));
    int key_len = char_to_int((u8 *)data[k], sizeof(KvKeyLen));
    ctx->ret_val.value_addr = (void *)((uint64_t)data[k] + 
        sizeof(KvKeyLen) + sizeof(KvValueLen) + key_len);
}

void ClientFR::kv_degrade_RTT5(KVReqCtx *ctx){
    kv_degrade_decoding(ctx);
}

void *ClientFR::kv_degread_sync(KVReqCtx *ctx){
    kv_degrade_RTT1(ctx);
    kv_degrade_RTT2(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        return NULL;
    }
    kv_degrade_RTT3(ctx);
    kv_degrade_RTT4(ctx);
    kv_degrade_RTT5(ctx);
    return ctx->ret_val.value_addr;
}

void ClientFR::kv_degrade_RTT1_async(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_degrade_read_object_index(ctx);
    int ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv degrade read:RTT1 send failed~");
        exit(0);
    }
}

void ClientFR::kv_degrade_RTT2_async(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_degrade_read_stripe_index(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        return;
    }
    int ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv degrade read:RTT2 send failed~");
        exit(0);
    }
}

void ClientFR::kv_degrade_RTT3_async(KVReqCtx *ctx){
    init_req_comp_wrid_map(ctx);
    kv_degrade_read_parity_chunk(ctx);
    int ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv degrade read:RTT3 send failed~");
        exit(0);
    }
}

void ClientFR::kv_degrade_get_kv_chain_walking_async(KVReqCtx *ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    StripeIndex *stripe_index = (StripeIndex *)drctx->stripe_index_addr;
    ObjectIndex *object_index = (ObjectIndex *)drctx->object_index_addr;
    uint64_t remote_addr;
    ChunkIndex *chunk_index;
    int ret;
    IbvSrList *sr_list;
    for(int i = 0;i < object_index->offset + 1;i ++){
        init_req_comp_wrid_map(ctx);
        for(int j = 0;j < k;j ++){
            if(object_index->chunk_index == j){
                continue;
            }
            if(i == 0){
                remote_addr = remote_chunk_index_addr + CHU_INDEX_LEN * stripe_index->cid[j];
                sr_list = gen_one_sr_list(drctx->related_kv_addr[j], CHU_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id,
                    0, DEGRADE_READ_CHUNK_INDEX, j, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
                send_one_sr_list(sr_list, &ctx->comp_wrid_map);
            } else {
                chunk_index = (ChunkIndex *)drctx->related_kv_addr[j];
                remote_addr = (uint64_t)chunk_index->next;
                sr_list = gen_one_sr_list(drctx->related_kv_addr[j], CHU_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id,
                    0, DEGRADE_READ_CHUNK_INDEX, j, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
                send_one_sr_list(sr_list, &ctx->comp_wrid_map);
            }
        }
        ret = poll_completion(ctx->comp_wrid_map);
        if(ret == -1){
            print_error("kv degrade read:RTT4 chain walking failed~");
            exit(0);
        }
    }
    uint8_t server_id;
    init_req_comp_wrid_map(ctx);
    for(int j = 0;j < k;j ++){
        if(object_index->chunk_index == j){
            continue;
        }
        chunk_index = (ChunkIndex *)drctx->related_kv_addr[j];
        server_id   = chunk_index->pmsg.server_id;
        remote_addr = chunk_index->pmsg.addr;
        sr_list = gen_one_sr_list(drctx->related_kv_addr[j], CHU_INDEX_LEN, local_buf_mr_->lkey, ctx->coro_id,
            server_id, DEGRADE_READ_RELATED_KV, j, IBV_WR_RDMA_READ, remote_addr, 0, NULL);
        send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    }
    ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("kv degrade read:RTT4 read related kv failed~");
        exit(0);
    }
}

void ClientFR::kv_degrade_RTT4_async(KVReqCtx *ctx){
    kv_degrade_get_kv_chain_walking_async(ctx);
}

void *ClientFR::kv_degrade_read(KVReqCtx *ctx){
    kv_degrade_RTT1_async(ctx);
    kv_degrade_RTT2_async(ctx);
    if(ctx->ret_val.ret_code == KV_OPS_FAIL_RETURN){
        return NULL;
    }
    kv_degrade_RTT3_async(ctx);
    kv_degrade_RTT4_async(ctx);
    kv_degrade_RTT5(ctx);
    return ctx->ret_val.value_addr;
}

IbvSrList *ClientFR::gen_one_sr_list(uint64_t local_addr, uint32_t local_len, uint32_t lkey, 
    uint32_t coro_id, uint8_t server_id, uint32_t type, uint32_t wr_id_offset, 
    ibv_wr_opcode opcode, uint64_t remote_addr, int send_flags, ibv_send_wr *next){
    IbvSrList          * sr_list = get_one_sr_list();
    struct ibv_send_wr * sr      = get_one_sr();
    struct ibv_sge     * sge     = get_one_sge();
    sge->addr = local_addr;
    sge->length = local_len;
    sge->lkey = lkey;
    sr->wr_id = ib_gen_wr_id(coro_id, server_id, type, wr_id_offset);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode = opcode;
    sr->wr.rdma.remote_addr = remote_addr;
    sr->wr.rdma.rkey        = server_mr_info_map_[server_id]->rkey;
    sr->send_flags = send_flags;
    sr->next = next;
    sr_list->num_sr    = 1;
    sr_list->server_id = server_id;
    sr_list->sr_list   = sr;
    return sr_list;
}

void ClientFR::send_one_sr_list(IbvSrList * sr_list, map<uint64_t, bool> *comp_wrid_map){
    uint32_t num_sr = sr_list[0].num_sr;
    sr_list[0].sr_list[num_sr - 1].send_flags |= IBV_SEND_SIGNALED;
    if(comp_wrid_map->find(sr_list[0].sr_list[num_sr - 1].wr_id) != comp_wrid_map->end()){
        RDMA_LOG_IF(2, if_print_log) << "wr id: " << 
            sr_list[0].sr_list[num_sr - 1].wr_id << " complicate~";
    }
    comp_wrid_map[0][sr_list[0].sr_list[num_sr - 1].wr_id] = false;
    struct ibv_qp * send_qp = nm_->rc_qp_list_[sr_list[0].server_id]; 
    struct ibv_send_wr * bad_wr;
    ibv_post_send(send_qp, sr_list[0].sr_list, &bad_wr);
}

IbvSrList *ClientFR::gen_one_cas_sr_list(uint64_t local_addr, uint32_t lkey, uint32_t coro_id, uint8_t server_id,
    uint32_t type, uint32_t wr_id_offset, uint64_t remote_addr, uint64_t origin_value, 
    uint64_t new_value, int send_flags, ibv_send_wr *next){
    IbvSrList *sr_list = get_one_sr_list();
    struct ibv_send_wr * sr = get_one_sr();
    struct ibv_sge     *sge = get_one_sge();
    sge->addr = local_addr;
    sge->length = CAS_SIZE;
    sge->lkey = lkey;
    sr->wr_id = ib_gen_wr_id(coro_id, server_id, type, wr_id_offset);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr->wr.atomic.remote_addr = remote_addr;
    sr->wr.atomic.rkey = server_mr_info_map_[server_id]->rkey;
    sr->wr.atomic.compare_add = origin_value;
    sr->wr.atomic.swap = new_value;
    sr->send_flags = send_flags;
    sr->next = next;
    sr_list->num_sr = 1;
    sr_list->server_id = server_id;
    sr_list->sr_list = sr;
    return sr_list;   
}

IbvSrList * ClientFR::gen_write_kv_sr_lists_encoding(uint32_t coro_id, KVInfo * a_kv_info, 
    ClientMMAllocCtx * r_mm_info, __OUT uint32_t * num_sr_lists, int id) {
    IbvSrList * ret_sr_list = get_one_sr_list_encoding();
    struct ibv_send_wr * sr = get_one_sr_encoding();
    struct ibv_sge     * sge = get_one_sge_encoding();
    sge->addr   = (uint64_t)a_kv_info->l_addr;
    sge->length = r_mm_info->num_subblocks * mm_->subblock_sz_;
    sge->lkey   = a_kv_info->lkey;
    sr->wr_id   = ib_gen_wr_id(coro_id, r_mm_info->server_id_list, id, 1);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode  = IBV_WR_RDMA_WRITE;
    sr->wr.rdma.remote_addr = r_mm_info->addr_list;
    sr->wr.rdma.rkey        = r_mm_info->rkey_list;
    sr->next    = NULL;
    ret_sr_list->sr_list   = sr;
    ret_sr_list->num_sr    = 1;
    ret_sr_list->server_id = r_mm_info->server_id_list;
    *num_sr_lists = 1;
    return ret_sr_list;
}

IbvSrList * ClientFR::gen_read_kv_sr_lists(uint32_t coro_id, const std::vector<KVRWAddr> & r_addr_list, 
    __OUT uint32_t * num_sr_lists) {
    std::map<uint8_t, std::vector<KVRWAddr> > server_id_kv_addr_map;
    for (size_t i = 0; i < r_addr_list.size(); i ++) {
        server_id_kv_addr_map[r_addr_list[i].server_id].push_back(r_addr_list[i]);
    }
    IbvSrList * ret_sr_list = (IbvSrList *)malloc(sizeof(IbvSrList) * server_id_kv_addr_map.size());
    std::map<uint8_t, std::vector<KVRWAddr> >::iterator it;
    uint32_t sr_num_cnt = 0;
    int index = 0;
    for (it = server_id_kv_addr_map.begin(); it != server_id_kv_addr_map.end(); it++) {
        size_t cur_num_sr = it->second.size();
        struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * cur_num_sr);
        struct ibv_sge     * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * cur_num_sr);
        memset(sr, 0, sizeof(struct ibv_send_wr) * cur_num_sr);
        memset(sge, 0, sizeof(struct ibv_sge) * cur_num_sr);
        for (size_t i = 0; i < it->second.size(); i ++) {
            sge[i].addr   = it->second[i].l_kv_addr;
            sge[i].length = it->second[i].length;
            sge[i].lkey   = it->second[i].lkey;
            sr[i].wr_id =  ib_gen_wr_id(coro_id, it->first, READ_KV_ST_WRID, index + 1);
            sr[i].sg_list = &sge[i];
            sr[i].num_sge = 1;
            sr[i].opcode  = IBV_WR_RDMA_READ;
            sr[i].wr.rdma.remote_addr = it->second[i].r_kv_addr;
            sr[i].wr.rdma.rkey = it->second[i].rkey;
            sr[i].send_flags = 0;
            if (i != it->second.size() - 1) {
                sr[i].next = &sr[i + 1];
            }
            index ++;
        }
        ret_sr_list[sr_num_cnt].sr_list   = sr;
        ret_sr_list[sr_num_cnt].server_id = it->first;
        ret_sr_list[sr_num_cnt].num_sr    = it->second.size();
        sr_num_cnt ++;
    }
    *num_sr_lists = server_id_kv_addr_map.size();
    return ret_sr_list;
}

IbvSrList * ClientFR::gen_write_kv_sr_lists(uint32_t coro_id, KVInfo * a_kv_info, 
    ClientMMAllocCtx * r_mm_info, __OUT uint32_t * num_sr_lists, int id) {
    IbvSrList * ret_sr_list = get_one_sr_list();
    struct ibv_send_wr * sr = get_one_sr();
    struct ibv_sge     * sge = get_one_sge();
    sge->addr   = (uint64_t)a_kv_info->l_addr;
    sge->length = r_mm_info->num_subblocks * mm_->subblock_sz_;
    sge->lkey   = a_kv_info->lkey;
    sr->wr_id   = ib_gen_wr_id(coro_id, r_mm_info->server_id_list, id, 1);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode  = IBV_WR_RDMA_WRITE;
    sr->wr.rdma.remote_addr = r_mm_info->addr_list;
    sr->wr.rdma.rkey        = r_mm_info->rkey_list;
    sr->send_flags = 0;
    sr->next    = NULL;
    ret_sr_list->sr_list   = sr;
    ret_sr_list->num_sr    = 1;
    ret_sr_list->server_id = r_mm_info->server_id_list;
    *num_sr_lists = 1;
    return ret_sr_list;
}

void ClientFR::init_time_latency(int num_op){
    clock_gettime(CLOCK_REALTIME, pre_time);
    op_id = 0;
}

void ClientFR::get_time_latency(bool if_failed){
    struct timespec old_time = *pre_time;
    clock_gettime(CLOCK_REALTIME, pre_time);
    if(if_failed){
        return;
    }
    lat_list[op_id] = (double)((pre_time->tv_sec - old_time.tv_sec) * 1000000000 + 
        (pre_time->tv_nsec - old_time.tv_nsec)) / 1000.0;
    // cout << "op: " << op_id << " lat:" << lat_list[op_id] << endl;
    op_id ++;
}

void ClientFR::print_time_latency(){
    double total_latency = 0;
    for(int i = 0;i < op_id;i ++){
        total_latency += (double)lat_list[i];
    }
    print_args("total latency", total_latency);
    print_args("num operation", num_local_operations_);
    print_args("num failed", num_failed_);
    double ave_lat = (double)total_latency / (double)(num_local_operations_ - num_failed_);
    print_args("average latency(us)", ave_lat);

}

void ClientFR::req_init_sr_list(KVReqCtx *ctx){
    ctx->sr_list_batch.clear();
    ctx->sr_list_num_batch.clear();
}

int ClientFR::req_batch_send_and_wait(KVReqCtx *ctx){
    ibv_wc wc;
    int ret = nm_->rdma_post_sr_list_batch_sync(ctx->sr_list_batch, ctx->sr_list_num_batch, &wc);
    return ret;
}

void ClientFR::req_append_sr_list(KVReqCtx *ctx, IbvSrList *sr_list){
    ctx->sr_list_batch.push_back(sr_list);
    ctx->sr_list_num_batch.push_back(1);
}

pthread_t ClientFR::start_polling_thread() {
    NMPollingThreadArgs * args = (NMPollingThreadArgs *)malloc(sizeof(NMPollingThreadArgs));
    args->nm = nm_;
    args->core_id = poll_core_id_;
    pthread_t polling_tid;
    pthread_create(&polling_tid, NULL, nm_polling_thread, (void *)args);
    return polling_tid;
}

void ClientFR::stop_polling_thread() {
    nm_->stop_polling();
}

void * client_ops_fb_cnt_ops_cont_fr(void * arg) {
    boost::this_fiber::yield();
    ClientFRFiberArgs * fiber_args = (ClientFRFiberArgs *)arg;
    ClientFR *client = fiber_args->client;
    RDMA_LOG_IF(4, client->if_print_log) << "start opertion fiber!";
    if (fiber_args->ops_cnt == 0) {
        client->init_kvreq_space(fiber_args->coro_id, fiber_args->ops_st_idx, fiber_args->ops_num);
    }
    int ret = 0;
    void * search_addr = NULL;
    fiber_args->b->wait();
    boost::this_fiber::yield();
    uint32_t num_failed = fiber_args->num_failed;
    uint32_t cnt = fiber_args->ops_cnt;
    std::unordered_map<std::string, bool> inserted_key_map;
    fiber_args->client->num_test = 0;
    int count = 0;
    bool if_con;
    while (*fiber_args->should_stop == false && fiber_args->ops_num != 0) {
        uint32_t idx = cnt % fiber_args->ops_num;
        KVReqCtx * ctx = &client->kv_req_ctx_list_[idx + fiber_args->ops_st_idx];
        ctx->ctx_id = idx + fiber_args->ops_st_idx;
        ctx->should_stop = fiber_args->should_stop;
        ctx->mm_alloc_ctx.addr_list = 1;
        ctx->ret_val.ret_code = KV_OPS_SUCCESS;
        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            // break;
            search_addr = client->kv_search(ctx);
            if (search_addr == NULL) {
                num_failed ++;
            }
            // char value[32];
            // memcpy(value, search_addr, 32);
            // RDMA_LOG_IF(2, fiber_args->client->if_print_log) << "search value:" << value;
            count ++;
            break;
        case KV_DEGRADE_READ:
            search_addr = client->kv_degrade_read(ctx);
            if (search_addr == NULL) {
                num_failed ++;
            } else {
                count ++;
                // RDMA_LOG_IF(2, fiber_args->client->if_print_log) 
                //     << "degrade read value:";
                // RDMA_LOG_IF(2, fiber_args->client->if_print_log)
                //     .write((char *)search_addr, fiber_args->client->value_size);
            }
            break;
        case KV_REQ_UPDATE:
            ret = fiber_args->client->kv_update_async(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            count ++;
            break;
        default:
            client->kv_search(ctx);
            break;
        }
        if (ret == KV_OPS_FAIL_REDO) {
            RDMA_LOG_IF(2, fiber_args->client->if_print_log) << "failed!";
            RDMA_LOG_IF(2, fiber_args->client->if_print_log) << "ctx->key_str:" << ctx->key_str;
            exit(0);
            cnt --;
        }
        cnt ++;
        fiber_args->client->num_test ++;
    }
    // cout << "num failed:" << num_failed << endl;
    fiber_args->ops_cnt = count;
    fiber_args->num_failed = num_failed;
    return NULL;
}