#include "client.h"

CasRecovery::CasRecovery(int num_coro){
    this->num_coro = num_coro;
    cas_rec_ele_queue = new queue<CasRecoveryEle>[num_coro];
}

CasRecovery::~CasRecovery(){
    delete cas_rec_ele_queue;
}

void CasRecovery::insert(int & coro_id, CasRecoveryEle *ele){
    cas_rec_ele_queue[coro_id].push(*ele);
}

void CasRecovery::remove(int coro_id){
    cas_rec_ele_queue[coro_id].pop();
}

CasRecoveryEle *CasRecovery::check_coro(uint32_t & coro_id){
    queue<CasRecoveryEle> *cas_rec = &cas_rec_ele_queue[coro_id];
    if(cas_rec->size() != 0){
        CasRecoveryEle *cas_rec_ele = &cas_rec->front();
        if(cas_rec_ele->recovery_times > 0 && cas_rec_ele->recovery_when == 0){
            return cas_rec_ele;
        } else {
            cas_rec_ele->recovery_when --;
        }
    }
    return NULL;
}

Client::Client(struct GlobalConfig * conf) {
    gettimeofday(&recover_st_, NULL);
    is_recovery = conf->is_recovery;
    init_count_used_slot();
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
    cas_rec = new CasRecovery(num_coroutines_);
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
    mm_ = new ClientMM(conf, nm_);
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
    while(local_buf_ == MAP_FAILED){
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
        while(input_buf_ == MAP_FAILED){
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
    encoding_thread.init(1);
    encoding_thread.start();
    print_mes("encoding thread init~");

    // log
    init_log();
    print_mes("init log~");

    // add pre malloc sr lists
    init_sr_list();
    print_mes("per malloc sr list~");

    // init race root hash table
    gettimeofday(&ec_init_et_, NULL);
    init_all_hash_index();
    print_mes("Init client finished~");

    // add update breakdown bottlenect 
    if_req_latency = conf->if_req_latency;
    print_args("if req latency", if_req_latency);
}

Client::~Client() {
    delete nm_;
    // delete mm_;
}

uint16_t combined_server_id(vector<uint8_t> & server_list){
    uint16_t ret = 0;
    int num_ = server_list.size();
    sort(server_list.begin(), server_list.end());
    for(int i = 0;i < num_;i ++){
        ret += server_list[i] * pow(10, i);
    }
    return ret;
}

void Client::init_other_client_PL_map(){
    int num_com = hashring_table->PL_map_list_->num_combined;
    hashring_table->PL_map = new map<uint16_t, uint8_t>;
    vector<uint8_t> server_list;
    for(int i = 0;i < num_com;i ++){
        for(int j = 0;j < m;j ++){
            server_list.push_back(hashring_table->PL_map_list_->PL_map_element[i][j]);
        }
        (*hashring_table->PL_map)[combined_server_id(server_list)] = i;
        server_list.clear();
    }
}

void Client::read_all_hash_table(){
    print_mes("get remote hash root~");
    int ret;
    // get all hash root
    ret = nm_->nm_rdma_read_from_sid((void *)hashring_table->kv_ec_meta_hash_root_, 
        hashring_table->kv_ec_meta_hash_root_mr_->lkey, ROOT_KV_META_LEN,
        remote_kv_ec_meta_root_addr_, server_mr_info_map_[0]->rkey, 0);
    ret = nm_->nm_rdma_read_from_sid((void *)hashring_table->stripe_meta_hash_root_, 
        hashring_table->stripe_meta_hash_root_mr_->lkey, ROOT_STRIPE_META_LEN,
        remote_stripe_meta_root_addr_, server_mr_info_map_[0]->rkey, 0);
    ret = nm_->nm_rdma_read_from_sid((void *)hashring_table->PL_hash_root_, 
        hashring_table->PL_hash_root_mr_->lkey, ROOT_PL_LEN,
        remote_PL_root_addr_, server_mr_info_map_[0]->rkey, 0);
    ret = nm_->nm_rdma_read_from_sid((void *)hashring_table->metadata_hash_root_, 
        hashring_table->metadata_hash_root_mr_->lkey, ROOT_METADATA_LEN,
        remote_metadata_root_addr_, server_mr_info_map_[0]->rkey, 0);
    ret = nm_->nm_rdma_read_from_sid((void *)hashring_table->PL_map_list_,
        hashring_table->PL_map_list_mr_->lkey, PLMAPLIST_SIZE, 
        remote_PL_map_addr, server_mr_info_map_[0]->rkey, 0);
    init_other_client_PL_map();  
    print_mes("get remote hash root finished~");
}

void Client::write_all_hash_table(){
    print_mes("write all hash table~");
    int ret;
    for (int i = 0; i < num_memory_; i ++) {
        ret = nm_->nm_rdma_write_to_sid((void *)hashring_table->kv_ec_meta_hash_root_, 
            hashring_table->kv_ec_meta_hash_root_mr_->lkey, sizeof(HashRootKvEcMeta),
            remote_kv_ec_meta_root_addr_, server_mr_info_map_[i]->rkey, i);
        ret = nm_->nm_rdma_write_to_sid((void *)hashring_table->stripe_meta_hash_root_, 
            hashring_table->stripe_meta_hash_root_mr_->lkey, sizeof(HashRootStripeMeta),
            remote_stripe_meta_root_addr_, server_mr_info_map_[i]->rkey, i);
        ret = nm_->nm_rdma_write_to_sid((void *)hashring_table->PL_hash_root_, 
            hashring_table->PL_hash_root_mr_->lkey, sizeof(HashRootPL),
            remote_PL_root_addr_, server_mr_info_map_[i]->rkey, i); 
        ret = nm_->nm_rdma_write_to_sid((void *)hashring_table->metadata_hash_root_, 
            hashring_table->metadata_hash_root_mr_->lkey, sizeof(HashRootMetaData),
            remote_metadata_root_addr_, server_mr_info_map_[i]->rkey, i);
        ret = nm_->nm_rdma_write_to_sid((void *)hashring_table->PL_map_list_,
            hashring_table->PL_map_list_mr_->lkey, PLMAPLIST_SIZE, 
            remote_PL_map_addr, server_mr_info_map_[i]->rkey, i);
    }
    print_mes("write all hash table finished~");
}

void Client::init_all_hash_table_mem(){
    IbInfo ib_info;
    nm_->get_ib_info(&ib_info);
    // all hash table allocate
    hashring_table->kv_ec_meta_hash_root_ = (HashRootKvEcMeta *) malloc (ROOT_KV_META_LEN);
    hashring_table->stripe_meta_hash_root_ = (HashRootStripeMeta *) malloc (ROOT_STRIPE_META_LEN);
    hashring_table->PL_hash_root_ = (HashRootPL *) malloc (ROOT_PL_LEN);
    hashring_table->metadata_hash_root_ = (HashRootMetaData *) malloc (ROOT_METADATA_LEN);
    hashring_table->PL_map_list_ = (PLMapList *) malloc (PLMAPLIST_SIZE);
    hashring_table->kv_ec_meta_hash_root_mr_ = ibv_reg_mr(ib_info.ib_pd, hashring_table->kv_ec_meta_hash_root_,
        ROOT_KV_META_LEN, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    hashring_table->stripe_meta_hash_root_mr_ = ibv_reg_mr(ib_info.ib_pd, hashring_table->stripe_meta_hash_root_,
        ROOT_STRIPE_META_LEN, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    hashring_table->PL_hash_root_mr_ = ibv_reg_mr(ib_info.ib_pd, hashring_table->PL_hash_root_,
        ROOT_PL_LEN, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    hashring_table->metadata_hash_root_mr_ = ibv_reg_mr(ib_info.ib_pd, hashring_table->metadata_hash_root_,
        ROOT_METADATA_LEN, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    hashring_table->PL_map_list_mr_ = ibv_reg_mr(ib_info.ib_pd, hashring_table->PL_map_list_, 
        PLMAPLIST_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

    // all hash table remote addr
    remote_PL_map_addr = remote_sync_addr + sizeof(uint64_t);
    remote_kv_ec_meta_root_addr_ = remote_global_meta_addr_ + META_AREA_LEN + GC_AREA_LEN;
    remote_stripe_meta_root_addr_ = remote_kv_ec_meta_root_addr_ + roundup_256(ROOT_KV_META_LEN);
    remote_PL_root_addr_ = remote_stripe_meta_root_addr_ + roundup_256(ROOT_STRIPE_META_LEN);
    remote_metadata_root_addr_ = remote_PL_root_addr_ + roundup_256(ROOT_PL_LEN);
}

int Client::init_all_hash_index(){
    hashring_table = new HashRingTable;
    int ret = 0;
    init_all_hash_table_mem();
    if(is_recovery == false){
        if (client_id - num_memory_ == 0) {
            read_all_hash_table();
            ret = init_all_hash_subtable();
            write_all_hash_table();
            ret = sync_init_finish();
        } else {
            while (!init_is_finished());
            read_all_hash_table();
        }
    } else {
        read_all_hash_table();
        // add read log and recovery
        recovery();
        gettimeofday(&kv_ops_recover_et_, NULL);
    }
    print_mes("Init all subtable finished~");
    return ret;
}

void Client::recovery_clear_sr_list(RecoveryCtx *rcctx){
    rcctx->sr_list_batch.clear();
    rcctx->sr_list_num_batch.clear();
}

void Client::recovery_init_rcctx(RecoveryCtx *rcctx){
    rcctx->server_buf_addr = new uint64_t[num_memory_];
    rcctx->server_num_log = new int[num_memory_];
    IbInfo ib_info;
    nm_->get_ib_info(&ib_info);
    rcctx->rec_buf_ = malloc(RECOVERY_BUF_LEN);
    rcctx->rec_buf_mr_ = ibv_reg_mr(ib_info.ib_pd, rcctx->rec_buf_, RECOVERY_BUF_LEN,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    for(int i = 0;i < num_memory_;i ++){
        rcctx->server_buf_addr[i] = (uint64_t)rcctx->rec_buf_ + i * RECOVERY_MN_BUF_LEN;
    }
    rcctx->num_common_op = 0;
    rcctx->num_failed_op = 0;
    rcctx->num_op = 0;
}

void Client::recovery_read_wp_rp(RecoveryCtx *rcctx){
    recovery_clear_sr_list(rcctx);
    IbvSrList *sr_list;
    uint32_t sr_list_num;
    for(int i = 0;i < num_memory_;i ++){
        // get wp
        sr_list = gen_sr_list(rcctx->server_buf_addr[i], sizeof(uint64_t), rcctx->rec_buf_mr_->lkey,
            0, i, RECOVERY_READ_WP, i, IBV_WR_RDMA_READ, client_log->log_list[i].remote_wp_addr);
        rcctx->sr_list_batch.push_back(sr_list);
        rcctx->sr_list_num_batch.push_back(1);
        // get rp
        sr_list = gen_sr_list(rcctx->server_buf_addr[i] + sizeof(uint64_t), sizeof(uint64_t), rcctx->rec_buf_mr_->lkey,
            0, i, RECOVERY_READ_RP, i, IBV_WR_RDMA_READ, client_log->log_list[i].remote_rp_addr);
        rcctx->sr_list_batch.push_back(sr_list);
        rcctx->sr_list_num_batch.push_back(1);
    }
    nm_->rdma_post_sr_list_batch_sync(rcctx->sr_list_batch, rcctx->sr_list_num_batch, &rcctx->wc);
    gettimeofday(&get_wp_rp_middle_et_, NULL);
    // print_mes("print read wp and rp~");
    for(int i = 0;i < num_memory_;i ++){
        // cout << "server:" << i << " wp:" << *(uint64_t *)(rcctx->server_buf_addr[i]) << endl;
        // cout << "server:" << i << " rp:" << *(uint64_t *)(rcctx->server_buf_addr[i] + sizeof(uint64_t)) << endl;
        client_log->log_list[i].wp = *(uint64_t *)(rcctx->server_buf_addr[i]);
        client_log->log_list[i].rp = *(uint64_t *)(rcctx->server_buf_addr[i] + sizeof(uint64_t));
        rcctx->server_num_log[i] = client_log->log_list[i].wp - client_log->log_list[i].rp;
        if(rcctx->server_num_log[i] != 0){
            rcctx->num_op += rcctx->server_num_log[i];
            rcctx->num_common_op += rcctx->server_num_log[i] - 1;
            rcctx->num_failed_op ++;
        }
    }
    num_local_operations_ = rcctx->num_op;
    // cout << "num op:" << rcctx->num_op << endl;
    // cout << "num common op:" << rcctx->num_common_op << endl;
    // cout << "num failed op:" << rcctx->num_failed_op << endl;
    rcctx->kv_req_ctx = new KVReqCtx[rcctx->num_op];
    rcctx->kv_info = new KVInfo[rcctx->num_op];
    for(int i = 0;i < rcctx->num_op;i ++){
        rcctx->kv_req_ctx[i].kv_info = &rcctx->kv_info[i];
    }
}

void Client::recovery_read_all_log_entry(RecoveryCtx *rcctx){
    recovery_clear_sr_list(rcctx);
    IbvSrList *sr_list;
    uint32_t sr_list_num;
    for(int i = 0;i < num_memory_;i ++){
        sr_list = gen_sr_list(rcctx->server_buf_addr[i] + sizeof(uint64_t) * 2, 
            rcctx->server_num_log[i] * LOG_ENTRY_LEN, rcctx->rec_buf_mr_->lkey, 0, 
            i, RECOVERY_READ_LOG_ENTRY, i, IBV_WR_RDMA_READ,
            client_log->log_list[i].remote_log_entry_addr + client_log->log_list[i].rp * 
            LOG_ENTRY_LEN);
        rcctx->sr_list_batch.push_back(sr_list);
        rcctx->sr_list_num_batch.push_back(1);
    }
    nm_->rdma_post_sr_list_batch_sync(rcctx->sr_list_batch, rcctx->sr_list_num_batch, &rcctx->wc);
    // print_mes("print log entry~");
    // LogEntry *log_entry;
    // RaceHashSlot *slot;
    // for(int i = 0;i < num_memory_;i ++){
    //     for(int j = 0;j < rcctx->server_num_log[i];j ++){
    //         log_entry = (LogEntry *)(rcctx->server_buf_addr[i] + sizeof(uint64_t) * 2 + j * LOG_ENTRY_LEN);
    //         slot = (RaceHashSlot *)&log_entry->entry_val;
    //         cout << "server:" << i << " log:" << " type:" << 
    //             (uint16_t)log_entry->op_type_uf << " value:" << endl;
    //         cout << "fp:" << (uint16_t)slot->fp << " server id:" << (uint16_t)slot->server_id <<
    //             " len:" << (uint16_t)slot->kv_len << " addr:" << 
    //             hash_index_convert_40_to_64_bits(slot->pointer) << endl;
    //     }
    // }
}

void Client::recovery_read_all_kv(RecoveryCtx *rcctx){
    recovery_clear_sr_list(rcctx);
    LogEntry *log_entry;
    RaceHashSlot *slot;
    int index = 0;
    uint64_t local_ptr = (uint64_t)input_buf_;
    IbvSrList *sr_list;
    for(int i = 0;i < num_memory_;i ++){
        if(rcctx->server_num_log[i] != 0){
            for(int j = 0;j < rcctx->server_num_log[i] - 1;j ++){
                log_entry = (LogEntry *)(rcctx->server_buf_addr[i] + sizeof(uint64_t) * 2 + j * LOG_ENTRY_LEN);
                slot = (RaceHashSlot *)&log_entry->entry_val;
                rcctx->kv_req_ctx[index].kv_all_len = slot->kv_len * mm_->subblock_sz_;
                rcctx->kv_info[index].l_addr = (void *)local_ptr;
                sr_list = gen_sr_list(local_ptr, rcctx->kv_req_ctx[index].kv_all_len, input_buf_mr_->lkey,
                    0, i, READ_KV, index, IBV_WR_RDMA_READ, hash_index_convert_40_to_64_bits(slot->pointer));
                rcctx->sr_list_batch.push_back(sr_list);
                rcctx->sr_list_num_batch.push_back(1);
                local_ptr += rcctx->kv_req_ctx[index].kv_all_len;
                index ++;
            }
        }
    }
    for(int i = 0;i < num_memory_;i ++){
        if(rcctx->server_num_log[i] != 0){
            log_entry = (LogEntry *)(rcctx->server_buf_addr[i] + sizeof(uint64_t) * 2 + 
                (rcctx->server_num_log[i] - 1) * LOG_ENTRY_LEN);
            slot = (RaceHashSlot *)&log_entry->entry_val;
            rcctx->kv_req_ctx[index].kv_all_len = slot->kv_len * mm_->subblock_sz_;
            rcctx->kv_info[index].l_addr = (void *)local_ptr;
            sr_list = gen_sr_list(local_ptr, rcctx->kv_req_ctx[index].kv_all_len, input_buf_mr_->lkey,
                    0, i, READ_KV, index, IBV_WR_RDMA_READ, hash_index_convert_40_to_64_bits(slot->pointer));
            rcctx->sr_list_batch.push_back(sr_list);
            rcctx->sr_list_num_batch.push_back(1);
            local_ptr += rcctx->kv_req_ctx[index].kv_all_len;
            index ++;
        }
    }
    nm_->rdma_post_sr_list_batch_sync(rcctx->sr_list_batch, rcctx->sr_list_num_batch, &rcctx->wc);
    print_mes("print read kv chunk by log~");
    u8 *key_len_addr;
    u8 *value_len_addr;
    void *key_addr;
    void *value_addr;
    KvTail *tail_addr;
    for(int i = 0;i < rcctx->num_op;i ++){
        key_len_addr = (u8 *)rcctx->kv_info[i].l_addr;
        value_len_addr = (u8 *)((uint64_t)rcctx->kv_info[i].l_addr + sizeof(KvKeyLen));
        rcctx->kv_info[i].key_len  = char_to_int((u8 *)key_len_addr, sizeof(KvKeyLen));
        rcctx->kv_info[i].value_len  = char_to_int((u8 *)value_len_addr, sizeof(KvValueLen));
        rcctx->kv_info[i].lkey = input_buf_mr_->lkey;
        init_kv_req_ctx(&rcctx->kv_req_ctx[i], "INSERT");
        // cout << "key str:" << rcctx->kv_req_ctx[i].key_str << endl;
    }
}

void Client::recovery_init_all_req_ctx(RecoveryCtx *rcctx){
    kv_req_ctx_list_ = rcctx->kv_req_ctx;
    init_kvreq_space(0, 0, num_local_operations_);
}

void Client::recovery_redo_all_req(RecoveryCtx *rcctx){
    for(int i = 0;i < rcctx->num_common_op;i ++){
        kv_insert_only_buffer(&rcctx->kv_req_ctx[i]);
    }
    for(int i = rcctx->num_common_op;i < rcctx->num_op;i ++){
        kv_insert_posibility_crash_sync(&rcctx->kv_req_ctx[i]);
    }
}

void Client::recovery(){
    RecoveryCtx rcctx;
    // init recovery ctx request
    recovery_init_rcctx(&rcctx);
    // get all server wp and rp
    gettimeofday(&middle_et_, NULL);
    recovery_read_wp_rp(&rcctx);
    gettimeofday(&get_wp_rp_et_, NULL);
    // get all server log entry
    recovery_read_all_log_entry(&rcctx);
    gettimeofday(&get_unicol_log_et, NULL);
    // get all key-value
    recovery_read_all_kv(&rcctx);
    // reconstruct all request
    recovery_init_all_req_ctx(&rcctx);
    // run all request
    recovery_redo_all_req(&rcctx);
}

void Client::init_sr_list(){
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

IbvSrList *Client::get_one_sr_list(){
    return sr_list_queue->dequeue_free();
}

ibv_send_wr *Client::get_one_sr(){
    return sr_queue->dequeue_free();
}

ibv_sge *Client::get_one_sge(){
    return sge_queue->dequeue_free();
}

ibv_send_wr *Client::get_double_sr(){
    return double_sr_queue->dequeue_free();
}

ibv_sge *Client::get_double_sge(){
    return double_sge_queue->dequeue_free();
}

IbvSrList *Client::get_one_sr_list_encoding(){
    return ectx->sr_list_queue->dequeue_free();
}

ibv_send_wr *Client::get_one_sr_encoding(){
    return ectx->sr_queue->dequeue_free();
}

ibv_sge *Client::get_one_sge_encoding(){
    return ectx->sge_queue->dequeue_free();
}

ibv_send_wr *Client::get_double_sr_encoding(){
    return ectx->double_sr_queue->dequeue_free();
}

ibv_sge *Client::get_double_sge_encoding(){
    return ectx->double_sge_queue->dequeue_free();
}

int Client::connect_ib_qps() {
    uint32_t num_servers = nm_->get_num_servers();
    for (int i = 0; i < num_servers; i ++) {
        struct MrInfo * gc_info = (struct MrInfo *)malloc(sizeof(struct MrInfo));
        nm_->client_connect_one_rc_qp(i, gc_info);
        server_mr_info_map_[i] = gc_info;
    }
    return 0;
}

int Client::init_log(){
    client_log = new ClientLog;
    client_log->num_log = num_memory_;
    client_log->log_list = (Log *) malloc (sizeof(Log) * num_memory_);
    uint8_t server_id;
    uint64_t remote_addr;
    ClientMMAllocLogCtx log_info;
    for(int i = 0;i < num_memory_;i ++){
        // get log message from server
        mm_->mm_alloc_log(nm_, &log_info);
        server_id = log_info.server_id;
        remote_addr = log_info.addr;
        client_log->log_list[server_id].wp = 0;
        client_log->log_list[server_id].rp = 0;
        client_log->log_list[server_id].remote_wp_addr = remote_addr + sizeof(uint64_t);
        client_log->log_list[server_id].remote_rp_addr = remote_addr + sizeof(uint64_t) * 2;
        client_log->log_list[server_id].remote_log_entry_addr = remote_addr + sizeof(uint64_t) * 3;
    }
    return 0;
}

void Client::mm_alloc_to_subtable_entry(RaceHashSubtableEntry & entry, ClientMMAllocSubtableCtx & mmctx){
    entry.local_depth = num_memory_;
    entry.lock = 0;
    entry.server_id = mmctx.server_id;
    hash_index_convert_64_to_40_bits(mmctx.addr, entry.pointer);
}

int Client::init_all_hash_subtable(){
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

    // stripe meta
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
                mm_->mm_alloc_small_subtable(nm_, &subtable_info, primary_server);
                mm_alloc_to_subtable_entry(hashring_table->stripe_meta_hash_root_->
                    subtable_entry[primary_server][k].subtable_entry_primary, subtable_info);
                // backup
                backup_mn_index = get_multidimensional_index(backup_num_mn, k);
                for(int l = 0;l < m;l ++){
                    backup_server[l] = small_hashring_backup[l]->mn_id[backup_mn_index[l]];
                    // mm alloc small subtable backup server
                    mm_->mm_alloc_small_subtable(nm_, &subtable_info, backup_server[l]);
                    mm_alloc_to_subtable_entry(hashring_table->stripe_meta_hash_root_
                        ->subtable_entry[primary_server][k].subtable_entry_rep[l], subtable_info);
                }
            }
        }
    }

    // PL 
    hashring_table->PL_map = new map<uint16_t, uint8_t>;
    int subtable_index = 0;
    vector<int> ring_index;
    for(int i = 0;i < k_m;i ++){
        ring_index.push_back(i);
    }
    vector<vector<int>> combinations = get_combinations(ring_index, m);
    // print the combination
    // for (const auto& combination : combinations) {
    //     for (int num : combination) {
    //         std::cout << num << " ";
    //     }
    //     std::cout << std::endl;
    // }
    vector<uint8_t> server_list;
    server_list.clear();
    // deal with every combination
    for (const auto& combination : combinations) {
        all_com_backup_mn = 1;
        backup_num_mn.clear();
        for(int i = 0;i < m;i ++){
            small_hashring_backup[i] = &hashring->small_hashring[combination[i]];
            pre_mn = small_hashring_backup[i]->num_mn;
            all_com_backup_mn *= pre_mn;
            backup_num_mn.push_back(pre_mn);
        }
        backup_mn_index.clear();
        for(int i = 0;i < all_com_backup_mn;i ++){
            backup_mn_index = get_multidimensional_index(backup_num_mn, i);
            for(int j = 0;j < m;j ++){
                backup_server[j] = small_hashring_backup[j]->mn_id[backup_mn_index[j]];
                server_list.push_back(backup_server[j]);
                // mm alloc small subtable backup server
                mm_->mm_alloc_small_subtable(nm_, &subtable_info, backup_server[j]);
                mm_alloc_to_subtable_entry(hashring_table->PL_hash_root_
                    ->subtable_entry[subtable_index].subtable_entry_PL[j], subtable_info);
                // sync to all clients
                hashring_table->PL_map_list_->PL_map_element[subtable_index][j] = backup_server[j];
            }
            (*hashring_table->PL_map)[combined_server_id(server_list)] = subtable_index;
            subtable_index ++;
            server_list.clear();
        }
    }
    hashring_table->PL_map_list_->num_combined = subtable_index;

    // metadata
    all_com_backup_mn = 1;
    backup_num_mn.clear();
    for(int i = 0;i < k_m;i ++){
        small_hashring_backup[i] = &hashring->small_hashring[i];
        pre_mn = small_hashring_backup[i]->num_mn;
        all_com_backup_mn *= pre_mn;
        backup_num_mn.push_back(pre_mn);
    }
    backup_mn_index.clear();
    for(int i = 0;i < all_com_backup_mn;i ++){
        backup_mn_index = get_multidimensional_index(backup_num_mn, i);
        for(int j = 0;j < k_m;j ++){
            backup_server[j] = small_hashring_backup[j]->mn_id[backup_mn_index[j]];
            mm_->mm_alloc_small_subtable(nm_, &subtable_info, backup_server[j]);
            mm_alloc_to_subtable_entry(hashring_table->metadata_hash_root_
                ->subtable_entry[i].subtable_entry_metadata[j], subtable_info);
        }
    }
    print_mes("init all hash subtable finished~");
    return 0;
}

bool Client::init_is_finished() {
    nm_->nm_rdma_read_from_sid(local_buf_, local_buf_mr_->lkey, 
        sizeof(uint64_t), remote_global_meta_addr_, server_mr_info_map_[0]->rkey, 0);
    uint64_t read_value = *(uint64_t *)local_buf_;
    return read_value == 1;
}

int Client::sync_init_finish() {
    print_mes("sync~");
    uint64_t local_msg = 1;
    nm_->nm_rdma_write_inl_to_sid(&local_msg, sizeof(uint64_t), 
        remote_global_meta_addr_, server_mr_info_map_[0]->rkey, 0);
    print_mes("sync finished~");
    return 0;
}

bool Client::test_sync_faa_async(){
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
    printf("test_sync_faa_async: about to post FAA wr_id=%llu\n", (unsigned long long)sr_list->sr_list[0].wr_id);
    std::map<uint64_t, bool> comp_wrid_map;
    send_one_sr_list(sr_list, &comp_wrid_map);
    printf("test_sync_faa_async: posted FAA, polling for completion...\n");
    sleep(2);
    poll_completion(comp_wrid_map);
    printf("test_sync_faa_async: FAA completed\n");
    return 0;
}

bool Client::test_sync_read_async(){
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
    int sync_read_iter = 0;
    while (1) {
        comp_wrid_map.clear();
        send_one_sr_list(sr_list, &comp_wrid_map);
        poll_completion(comp_wrid_map);
        uint64_t read_value = *(uint64_t *)local_buf_;
        sync_read_iter++;
        if ((sync_read_iter % 1000) == 0) {
            printf("sync read value: %llu expected: %llu iter: %d\n",
                   (unsigned long long)read_value,
                   (unsigned long long)(all_clients * num_cn),
                   sync_read_iter);
        }
        if (read_value == all_clients * num_cn) {
            break;
        }
    }
    return true;
}

bool Client::test_sync_read_async_double(){
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

bool Client::test_read(int size) {
    nm_->nm_rdma_read_from_sid(local_buf_, local_buf_mr_->lkey, 
        size, remote_global_meta_addr_, server_mr_info_map_[0]->rkey, 0);
    uint64_t *data_ = (uint64_t *)local_buf_;
    cout << "read value is :"<<endl;
    for(int i = 0;i < size / 8;i ++){
        cout << hex << "0x"<< data_[i] << " ";
    }
    return false;
}

bool Client::test_write(uint64_t local_msg, int size, uint64_t offset) {
    nm_->nm_rdma_write_inl_to_sid(&local_msg, size, 
        remote_global_meta_addr_ + offset, server_mr_info_map_[0]->rkey, 0);
    return 0;
}

bool Client::test_cas(uint64_t swap_value, uint64_t cmp_value) {
    nm_->nm_rdma_cas(local_buf_, local_buf_mr_->lkey, sizeof(uint64_t), 
        remote_global_meta_addr_, server_mr_info_map_[0]->rkey, 0, swap_value, cmp_value);
    return 0;
}

bool Client::test_faa(uint64_t add_value, uint64_t offset) {
    nm_->nm_rdma_faa(local_buf_, local_buf_mr_->lkey, sizeof(uint64_t), 
        remote_global_meta_addr_ + offset, server_mr_info_map_[0]->rkey, 0, add_value);
    return 0;
}

void Client::test_read_latency() {
    // int size = 512;
    // int num_size = 8;
    int size[8] = {8, 24, 64, 128, 256, 512, 1024, 1024 * 16};
    // test_write(200, size);
    for(int j = 0;j < 8;j ++){
        struct timeval st, et;
        gettimeofday(&st, NULL);
        for(int i = 0;i < 10000;i ++){
            test_read(size[j]);
        }
        gettimeofday(&et, NULL);
        cout << "cost: " << (et.tv_sec - st.tv_sec) * 1000000 + (et.tv_usec - st.tv_usec) << endl;
    }
    std::cout << "test finished!" << std::endl;
}

void Client::test_write_cas_read(){
    int size = sizeof(uint64_t);
    std::cout << "test CAS-------------\n";
    std::cout << "first write a value to server\n";
    test_write(8, size, 0);
    std::cout << "check if write right\n";
    test_read(size);
    std::cout << "set a value to local buffer\n";
    *(uint64_t *)local_buf_ = 7UL;
    std::cout << "local_buf:" << *(uint64_t *)local_buf_ << std::endl;
    std::cout << "test cas operation\n";
    test_cas(10UL, 8UL); 
    sleep(2);
    std::cout << "cas finish and test if cas right\n";
    std::cout << "local_buf:" << *(uint64_t *)local_buf_ << std::endl;
    test_read(size);
}

void Client::test_write_faa_read(){
    int size = sizeof(uint64_t) * 3;
    std::cout << "test FAA-------------\n";
    std::cout << "first write a value to server\n";
    test_write(-1, sizeof(uint64_t), 8);
    test_write(0, sizeof(uint64_t), 0);
    test_write(0, sizeof(uint64_t), 16);
    std::cout << "check if write right\n";
    test_read(size);
    std::cout << "test faa operation\n";
    uint64_t faa_value = 1;
    test_faa(faa_value, 8);
    sleep(2);
    std::cout << "faa:" << faa_value << " finish and test if cas right\n";
    // std::cout << "local_buf:" << *(uint64_t *)local_buf_ << std::endl;
    test_read(size);
    cout << "test faa" << endl;
}

void Client::init_kv_req_ctx(KVReqCtx * ctx, char * operation) {
    ctx->lkey = local_buf_mr_->lkey;
    ctx->kv_modify_pr_cas_list.resize(1);
    ctx->ret_val.ret_code = 0;
    char key_buf[128] = {0};
    memcpy(key_buf, (void *)((uint64_t)(ctx->kv_info->l_addr) + sizeof(KvKeyLen) + sizeof(KvValueLen)), ctx->kv_info->key_len);
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
    ctx->req_timer = new ReqTimer(ctx->req_type);
}

void Client::init_kv_req_ctx_plus(KVReqCtx * ctx, char * operation, uint64_t & input_buf_ptr, uint64_t & used_len){
    ctx->lkey = local_buf_mr_->lkey;
    ctx->kv_modify_pr_cas_list.resize(1);
    int key_s = ctx->kv_info->key_len;
    char key_buf[key_s];
    memcpy(key_buf, (void *)((uint64_t)(ctx->kv_info->l_addr) + KV_KEY_LEN + KV_VALUE_LEN), key_s);
    ctx->key_str = string(key_buf);
    if(strcmp(operation, "INSERT") == 0){
        ctx->req_type = KV_REQ_INSERT;
        input_buf_ptr += ctx->kv_all_len;
        used_len += ctx->kv_all_len;
    } else if(strcmp(operation, "SEARCH") == 0){
        ctx->req_type = KV_REQ_SEARCH;
        input_buf_ptr += ctx->kv_all_len;
        used_len += ctx->kv_all_len;
    } else if(strcmp(operation, "UPDATE") == 0){
        ctx->req_type = KV_REQ_UPDATE;
        load_kv_req_init_update(ctx);
        input_buf_ptr += ctx->kv_all_len + ctx->uctx->parity_logging_size;
        used_len += ctx->kv_all_len + ctx->uctx->parity_logging_size;
    } else if(strcmp(operation, "DELETE") == 0){
        ctx->req_type = KV_REQ_DELETE;
        load_kv_req_init_delete(ctx);
        input_buf_ptr += ctx->kv_all_len + ctx->dctx->parity_logging_size;
        used_len += ctx->kv_all_len + ctx->dctx->parity_logging_size;
    } else if(strcmp(operation, "DEGRADE") == 0){
        ctx->req_type = KV_DEGRADE_READ;
        load_kv_req_init_degrade_read(ctx);
        input_buf_ptr += ctx->kv_all_len;
        used_len += ctx->kv_all_len;
    } else {
        ctx->req_type = KV_REQ_SEARCH;
        input_buf_ptr += ctx->kv_all_len;
        used_len += ctx->kv_all_len;
    }
}

void Client::init_kv_req_ctx_plus_ycsb(KVReqCtx * ctx, char * operation, uint64_t & input_buf_ptr, uint64_t & used_len){
    ctx->lkey = local_buf_mr_->lkey;
    ctx->kv_modify_pr_cas_list.resize(1);
    int key_s = ctx->kv_info->key_len;
    char key_buf[key_s];
    memcpy(key_buf, (void *)((uint64_t)(ctx->kv_info->l_addr) + KV_KEY_LEN + KV_VALUE_LEN), key_s);
    ctx->key_str = string(key_buf);
    if(strcmp(operation, "INSERT") == 0){
        ctx->req_type = KV_REQ_INSERT;
        input_buf_ptr += ctx->kv_all_len;
        used_len += ctx->kv_all_len;
    } else if(strcmp(operation, "READ") == 0){
        ctx->req_type = KV_REQ_SEARCH;
        input_buf_ptr += ctx->kv_all_len;
        used_len += ctx->kv_all_len;
    } else if(strcmp(operation, "UPDATE") == 0){
        ctx->req_type = KV_REQ_UPDATE;
        load_kv_req_init_update(ctx);
        input_buf_ptr += ctx->kv_all_len + ctx->uctx->parity_logging_size;
        used_len += ctx->kv_all_len + ctx->uctx->parity_logging_size;
    } else if(strcmp(operation, "DELETE") == 0){
        ctx->req_type = KV_REQ_DELETE;
        load_kv_req_init_delete(ctx);
        input_buf_ptr += ctx->kv_all_len + ctx->dctx->parity_logging_size;
        used_len += ctx->kv_all_len + ctx->dctx->parity_logging_size;
    } else if(strcmp(operation, "DEGRADE") == 0){
        ctx->req_type = KV_DEGRADE_READ;
        load_kv_req_init_degrade_read(ctx);
        input_buf_ptr += ctx->kv_all_len;
        used_len += ctx->kv_all_len;
    } else {
        ctx->req_type = KV_REQ_SEARCH;
        input_buf_ptr += ctx->kv_all_len;
        used_len += ctx->kv_all_len;
    }
}

void Client::init_kv_insert_space(void * coro_local_addr, KVReqCtx * ctx) {
    ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 2 * sizeof(KvEcMetaBucket));
    ctx->local_cas_return_value_addr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t));
    ctx->op_laddr = (void *)((uint64_t)ctx->local_cas_return_value_addr + sizeof(uint64_t));
    ctx->insert_kv_log_buf_ = (LogEntry *)ctx->op_laddr;
    ctx->insert_faa_wp_return_addr = (void *)((uint64_t)ctx->insert_kv_log_buf_ + LOG_ENTRY_LEN);
}

void Client::init_kv_insert_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_insert_space(coro_local_addr, ctx);
}

void Client::init_kv_search_space(void * coro_local_addr, KVReqCtx * ctx) {
    ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    ctx->local_kv_addr     = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
}

void Client::init_kv_search_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_search_space(coro_local_addr, ctx);
}

void Client::init_kv_update_space(void * coro_local_addr, KVReqCtx * ctx) {
    ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 16 * sizeof(RaceHashBucket));
    ctx->local_cas_return_value_addr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t));
    ctx->op_laddr = (void *)((uint64_t)ctx->local_cas_return_value_addr + sizeof(uint64_t));
    ctx->local_kv_addr = ctx->op_laddr;
    UpdateCtx *uctx = ctx->uctx;
    // add update's coro val
    uctx->coro_id = ctx->coro_id;
    // add get stripe meta bucket message 
    uctx->stripe_meta_bucket_info.local_bucket_addr = (RaceHashBucket *)((uint64_t)ctx->local_kv_addr + 
        get_aligned_size(ctx->kv_info->key_len + ctx->kv_info->value_len + KV_OTHER_LEN, mm_->subblock_sz_));
    init_bucket_space_havekv(&uctx->stripe_meta_bucket_info);
    // add get parity logging bucket message (m)
    for(int i = 0;i < m;i ++){
        if(i == 0){
            uctx->PL_bucket_buffer[i].local_bucket_addr = (RaceHashBucket *)uctx->stripe_meta_bucket_info.op_laddr;
        } else {
            uctx->PL_bucket_buffer[i].local_bucket_addr = (RaceHashBucket *)uctx->PL_bucket_buffer[i - 1].op_laddr;
        }
        init_bucket_space_havekv(&uctx->PL_bucket_buffer[i]);
    }
    uint64_t ptr = (uint64_t)uctx->PL_bucket_buffer[m - 1].op_laddr;
    uctx->stripe_meta_log_buf_addr = ptr;
    ptr += LOG_ENTRY_LEN;
    uctx->stripe_meta_end_log_buf_addr = ptr;
    ptr += LOG_ENTRY_LEN;
    uctx->kv_slot_log_buf_addr = ptr;
    ptr += LOG_ENTRY_LEN;
    for(int i = 0;i < m;i ++){
        uctx->PL_slot_log_buf_addr[i] = ptr;
        ptr += LOG_ENTRY_LEN;
    }
    uctx->stripe_meta_log_return_addr = ptr;
    ptr += sizeof(uint64_t);
    uctx->kv_slot_log_return_addr = ptr;
    ptr += sizeof(uint64_t);
    for(int i = 0;i < m;i ++){
        uctx->PL_slot_log_return_addr[i] = ptr;
        ptr += sizeof(uint64_t);
    }
}

void Client::init_kv_update_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_update_space(coro_local_addr, ctx);
}

void Client::init_kv_delete_space(void * coro_local_addr, KVReqCtx * ctx) {
    ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 16 * sizeof(RaceHashBucket));
    ctx->local_cas_return_value_addr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t));
    ctx->op_laddr = (void *)((uint64_t)ctx->local_cas_return_value_addr + sizeof(uint64_t));
    ctx->local_kv_addr = ctx->op_laddr;
    ctx->local_cache_addr = ctx->local_kv_addr;
    uint64_t ptr = (uint64_t)ctx->local_kv_addr + 3 * get_aligned_size(ctx->kv_info->key_len + 
        ctx->kv_info->value_len + KV_OTHER_LEN, mm_->subblock_sz_);
    DeleteCtx *dctx = ctx->dctx;
    dctx->coro_id = ctx->coro_id;
    // add get stripe meta bucket message 
    init_bucket_space(dctx->stripe_meta_bucket_info, ptr);
    // add get parity logging bucket message (m)
    for(int i = 0;i < m;i ++){
        init_bucket_space(&dctx->PL_bucket_buffer[i], ptr);
        dctx->PL_bucket_buffer[i].local_cas_target_value_addr = (void *)ptr;
        ptr += sizeof(uint64_t);
    }
    dctx->stripe_meta_log_buf_addr = ptr;
    ptr += LOG_ENTRY_LEN;
    dctx->stripe_meta_end_log_buf_addr = ptr;
    ptr += LOG_ENTRY_LEN;
    dctx->kv_slot_log_buf_addr = ptr;
    ptr += LOG_ENTRY_LEN;
    for(int i = 0;i < dctx->num_idx_rep;i ++){
        dctx->kv_meta_log_buf_addr[i] = ptr;
        ptr += LOG_ENTRY_LEN;
    }
    for(int i = 0;i < m;i ++){
        dctx->PL_slot_log_buf_addr[i] = ptr;
        ptr += LOG_ENTRY_LEN;
    }
    dctx->stripe_meta_log_return_addr = ptr;
    ptr += sizeof(uint64_t);
    dctx->kv_slot_log_return_addr = ptr;
    ptr += sizeof(uint64_t);
    for(int i = 0;i < dctx->num_idx_rep;i ++){
        dctx->kv_meta_log_return_addr[i] = ptr;
        ptr += sizeof(uint64_t);
    }
    for(int i = 0;i < m;i ++){
        dctx->PL_slot_log_return_addr[i] = ptr;
        ptr += sizeof(uint64_t);
    }
}

void Client::init_kv_delete_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_delete_space(coro_local_addr, ctx);
}

void Client::init_bucket_space_havekv(BucketBuffer * bucket_buffer){
    bucket_buffer->local_cas_target_value_addr = (void *)((uint64_t)bucket_buffer->local_bucket_addr + 4 * sizeof(RaceHashBucket));
    bucket_buffer->local_kv_addr = bucket_buffer->local_cas_target_value_addr;
    bucket_buffer->local_cas_return_value_addr = (void *)((uint64_t)bucket_buffer->local_cas_target_value_addr + sizeof(uint64_t));
    bucket_buffer->op_laddr = (void *)((uint64_t)bucket_buffer->local_cas_return_value_addr + sizeof(uint64_t));
}

void Client::init_kv_degrade_read_space(void * coro_local_addr, KVReqCtx * ctx){
    ctx->use_cache = true;
    ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    ctx->local_cache_addr = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
    ctx->local_kv_addr = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
    ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
    ctx->local_cas_return_value_addr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t));
    ctx->op_laddr = (void *)((uint64_t)ctx->local_cas_return_value_addr + sizeof(uint64_t));
    DegradeReadCtx *drctx = ctx->drctx;
    drctx->coro_id = ctx->coro_id;
    // get kv meta bucket
    drctx->kv_meta_bucket_buffer->local_bucket_addr = 
        (RaceHashBucket *)ctx->op_laddr;
    init_bucket_space_havekv(drctx->kv_meta_bucket_buffer);
    // get stripe meta bucket
    drctx->stripe_meta_bucket_buffer->local_bucket_addr = 
        (RaceHashBucket *)drctx->kv_meta_bucket_buffer->op_laddr;
        init_bucket_space_havekv(drctx->stripe_meta_bucket_buffer);
    // get ec meta bucket
    for(int i =  0;i < k;i ++){
        if(i == 0){
            drctx->metadata_bucket_buffer[i].local_bucket_addr = 
            (RaceHashBucket *)drctx->stripe_meta_bucket_buffer->op_laddr;
        }
        else {
            drctx->metadata_bucket_buffer[i].local_bucket_addr = 
            (RaceHashBucket *)drctx->metadata_bucket_buffer[i - 1].op_laddr;
        }
        init_bucket_space_havekv(&drctx->metadata_bucket_buffer[i]);
    }
    drctx->P0_bucket_buffer->local_bucket_addr = 
    (RaceHashBucket *)drctx->metadata_bucket_buffer[k - 1].op_laddr;
    uint64_t ptr = (uint64_t)drctx->P0_bucket_buffer->local_bucket_addr;
    ptr += 4 * COMBINED_BUCKET_SIZE;
    drctx->P0_bucket_buffer->local_cas_target_value_addr = (void *)ptr;
    ptr += sizeof(uint64_t);
    drctx->P0_bucket_buffer->local_cas_return_value_addr = (void *)ptr;
    ptr += sizeof(uint64_t);
    drctx->P0_bucket_buffer->local_kv_addr = (void *)ptr;
    ptr += buffer->block_size;
    drctx->P0_bucket_buffer->op_laddr = (void *)ptr;
    drctx->PL_bucket_buffer->local_bucket_addr = 
    (RaceHashBucket *)((uint64_t)drctx->P0_bucket_buffer->op_laddr + buffer->block_size);
    init_bucket_space_havekv(drctx->PL_bucket_buffer);
    for(int i = 0;i < (k - 1) * drctx->repair_kv_num;i ++){
        if(i == 0){
            drctx->kv_bucket_buffer[i].local_bucket_addr = 
                (RaceHashBucket *)((uint64_t)drctx->PL_bucket_buffer->op_laddr + 
                (mm_->subblock_sz_ * 2) * 4 * RACE_HASH_ASSOC_NUM);
        } else {
            drctx->kv_bucket_buffer[i].local_bucket_addr = 
                (RaceHashBucket *)drctx->kv_bucket_buffer[i - 1].op_laddr;
        }
        init_bucket_space_havekv(&drctx->kv_bucket_buffer[i]);
    }
    drctx->ec_meta_buf_ = drctx->kv_bucket_buffer[(k - 1) * drctx->repair_kv_num - 1].op_laddr;
    for(int i = 0;i < k_m;i ++){
        *(drctx->encoding_data + i) = (u8 *)((uint64_t)drctx->ec_meta_buf_ + buffer->block_size * (k + i));
    }
    ptr = (uint64_t)drctx->encoding_data[k_m - 1] + buffer->block_size;
    drctx->stripe_meta_log_buf_addr = ptr;
    ptr += LOG_ENTRY_LEN;
    drctx->stripe_meta_log_return_addr = ptr;
    ptr += sizeof(uint64_t);
    drctx->stripe_meta_end_log_buf_addr = ptr;
    ptr += sizeof(uint64_t);
}

void Client::init_bucket_space(BucketBuffer *bucketbuffer, uint64_t & ptr){
    bucketbuffer->local_bucket_addr = (RaceHashBucket *)ptr;
    ptr += sizeof(RaceHashBucket) * 4;
    bucketbuffer->local_cas_return_value_addr = (void *)ptr;
    ptr += sizeof(uint64_t);
}

void Client::init_bucket_space_big_slot(BucketBuffer *bucketbuffer, uint64_t & ptr){
    bucketbuffer->local_bucket_addr = (RaceHashBucket *)ptr;
    ptr += sizeof(KvEcMetaBucket) * 4;
    bucketbuffer->local_cas_return_value_addr = (void *)ptr;
    ptr += sizeof(uint64_t);
}

void Client::init_kv_degrade_read_space(void * coro_local_addr, uint32_t kv_req_idx){
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_degrade_read_space(coro_local_addr, ctx);
}

void Client::init_kvreq_space(uint32_t coro_id, uint32_t kv_req_st_idx, uint32_t num_ops) { 
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

void Client::load_kv_req_init_update(KVReqCtx *ctx){
    ctx->ret_val.ret_code = 0;
    ctx->uctx = new UpdateCtx;
    UpdateCtx *uctx = ctx->uctx;
    uctx->cas_rec_ele = new CasRecoveryEle;
    uctx->stripe_meta_hash_info = (KVHashInfo *) malloc (sizeof(KVHashInfo));
    uctx->stripe_meta_addr_info = (KVTableAddrInfo *) malloc (sizeof(KVTableAddrInfo));
    uctx->stripe_meta_bucket_info.kv_modify_pr_cas_list.resize(1);
    uctx->PL_info = (KVInfo *) malloc (sizeof(KVInfo) * m);
    uctx->PL_hash_info = (KVHashInfo *) malloc (sizeof(KVHashInfo) * m);
    uctx->PL_addr_info = (KVTableAddrInfo *) malloc (sizeof(KVTableAddrInfo));
    uctx->PL_bucket_buffer = new BucketBuffer[m];
    for(int i = 0;i < m;i ++){
        uctx->PL_bucket_buffer[i].kv_modify_pr_cas_list.resize(1);
    }
    uctx->parity_logging_size = ctx->kv_all_len + sizeof(ParityLogging);
    uctx->parity_logging_data = new u8;
    for(int i = 0;i < m;i ++){
        uctx->PL_info[i].l_addr = (void *)((uint64_t)ctx->kv_info->l_addr + ctx->kv_all_len);
    }
    uctx->comp_wrid_map = new map<uint64_t, bool>;
    uctx->parity_id = new int[m];
    // add log
    uctx->PL_slot_log_buf_addr = new uint64_t[m];
    uctx->PL_slot_log_return_addr = new uint64_t[m];
    ctx->is_finished = false;
}

void Client::load_kv_req_init_delete(KVReqCtx *ctx){
    ctx->dctx = new DeleteCtx;
    DeleteCtx *dctx = ctx->dctx;
    int rep = m + 1;
    dctx->num_idx_rep = rep;
    dctx->cas_rec_ele = new CasRecoveryEle;
    dctx->kv_meta_ctx = new KvEcMetaCtx[rep];
    dctx->stripe_meta_hash_info = (KVHashInfo *) malloc (sizeof(KVHashInfo));
    dctx->stripe_meta_addr_info = (KVTableAddrInfo *) malloc (sizeof(KVTableAddrInfo));
    dctx->stripe_meta_bucket_info = new BucketBuffer;
    dctx->stripe_meta_bucket_info->kv_modify_pr_cas_list.resize(1);
    dctx->stripe_meta_ctx = new StripeMetaCtx;
    dctx->stripe_meta_idx = new BucketSlotIdx;
    dctx->PL_info = (KVInfo *) malloc (sizeof(KVInfo) * m);
    dctx->PL_hash_info = (KVHashInfo *) malloc (sizeof(KVHashInfo) * m);
    dctx->PL_addr_info = (KVTableAddrInfo *) malloc (sizeof(KVTableAddrInfo) * m);
    dctx->PL_bucket_buffer = new BucketBuffer[m];
    for(int i = 0;i < m;i ++){
        dctx->PL_info[i].l_addr = (u8 *)((uint64_t)ctx->kv_info->l_addr + ctx->kv_all_len);
        dctx->PL_bucket_buffer[i].kv_modify_pr_cas_list.resize(1);
    }
    dctx->parity_logging_size = ctx->kv_all_len + sizeof(ParityLogging);
    dctx->parity_logging_data = new u8;
    dctx->parity_logging_data = (u8 *)((uint64_t)ctx->kv_info->l_addr + dctx->parity_logging_size);
    dctx->mm_alloc_parity_logging_ctx = (ClientMMAllocCtx *) malloc (sizeof(ClientMMAllocCtx) * m);
    dctx->PL_match_idx = 0;
    dctx->comp_wrid_map = new map<uint64_t, bool>;
    dctx->parity_id = new int[m];
    dctx->coro_id = -1;
    dctx->is_cas = true;
    // log
    dctx->kv_meta_log_buf_addr = new uint64_t[rep];
    dctx->kv_meta_log_return_addr = new uint64_t[rep];
    dctx->PL_slot_log_buf_addr = new uint64_t[m];
    dctx->PL_slot_log_return_addr = new uint64_t[m];
    ctx->is_finished = false;
}

void Client::load_kv_req_init_degrade_read(KVReqCtx * ctx){
    ctx->drctx = new DegradeReadCtx;
    DegradeReadCtx *drctx = ctx->drctx;
    drctx->kv_meta_hash_info = (KVHashInfo *) malloc (sizeof(KVHashInfo));
    drctx->kv_meta_addr_info = (KVTableAddrInfo *) malloc (sizeof(KVTableAddrInfo));
    drctx->kv_meta_bucket_buffer = (BucketBuffer *) malloc (sizeof(BucketBuffer));
    drctx->stripe_meta_hash_info = (KVHashInfo *) malloc (sizeof(KVHashInfo));
    drctx->stripe_meta_addr_info = (KVTableAddrInfo *) malloc (sizeof(KVTableAddrInfo));
    drctx->stripe_meta_bucket_buffer = new BucketBuffer;
    drctx->stripe_meta_bucket_buffer->kv_modify_pr_cas_list.resize(1);
    drctx->ec_meta_info = (KVInfo *) malloc (sizeof(KVInfo) * k);
    drctx->metadata_hash_info = (KVHashInfo *) malloc (sizeof(KVHashInfo) * k_m);
    drctx->metadata_addr_info = (KVTableAddrInfo *) malloc (sizeof(KVTableAddrInfo));
    drctx->metadata_bucket_buffer = (BucketBuffer *) malloc (sizeof(BucketBuffer) * k);
    drctx->P0_info = (KVInfo *) malloc (sizeof(KVInfo));
    drctx->P0_hash_info = (KVHashInfo *) malloc (sizeof(KVHashInfo));
    drctx->P0_addr_info = (KVTableAddrInfo *) malloc (sizeof(KVTableAddrInfo));
    drctx->P0_bucket_buffer = (BucketBuffer *) malloc (sizeof(BucketBuffer));
    drctx->PL_hash_info = (KVHashInfo *) malloc (sizeof(KVHashInfo));
    drctx->PL_addr_info = (KVTableAddrInfo *) malloc (sizeof(KVTableAddrInfo));
    drctx->PL_bucket_buffer = (BucketBuffer *) malloc (sizeof(BucketBuffer));
    int repair_kv_num = 3;
    drctx->repair_kv_num = repair_kv_num;
    drctx->kv_info = (KVInfo *) malloc (sizeof(KVInfo) * (k - 1) * repair_kv_num);
    drctx->kv_hash_info = (KVHashInfo *) malloc (sizeof(KVHashInfo) * (k - 1) * repair_kv_num);
    drctx->kv_addr_info = (KVTableAddrInfo *) malloc (sizeof(KVTableAddrInfo) * (k - 1) * repair_kv_num);
    drctx->kv_bucket_buffer = (BucketBuffer *) malloc (sizeof(BucketBuffer) * (k - 1) * repair_kv_num);
    drctx->kv_read_metadata_addr_list = new vector<KVRWAddr>[k];
    drctx->stripe_meta_idx = new BucketSlotIdx;
    drctx->need_kv = new NeedKv[k - 1];
    drctx->stripe_meta_ctx = new StripeMetaCtx;
    drctx->kv_meta_ctx = new KvEcMetaCtx;
    drctx->comp_wrid_map = new map<uint64_t, bool>;
    drctx->comp_wrid_map->clear();
    drctx->encoding_data = new u8*[k_m];
    drctx->parity_id = new int[m];
}

int Client::load_kv_req_micro_latency(int num_op, const char * op){
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
            for(int j = 0;j < pre_stripe - 1;j ++){
                update_trace_queue.push(update_trace_key_list_per_stripe[j].front());
                update_trace_key_list_per_stripe[j].pop_front();
            }
        }
        num_total_operations_ = update_trace_queue.size();
        // num_total_operations_ = 1;
    } else if(strcmp(op, "DELETE") == 0){
        for(int i = 0;i < k * (buffer->block_size) / mm_->subblock_sz_;i ++){
            for(int j = 0;j < pre_stripe - 1;j ++){
                delete_trace_queue.push(delete_trace_key_list_per_stripe[j].front());
                delete_trace_key_list_per_stripe[j].pop_front();
            }
        }
        num_total_operations_ = delete_trace_queue.size();
        // num_total_operations_ = 1;
    } else if(strcmp(op, "DEGRADE") == 0){
        num_op = (buffer->block_size / mm_->subblock_sz_) * k;
        for(int i = 0;i < num_op;i ++){
            for(int j = 0;j < pre_stripe - 1;j ++){
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
            memcpy(key_buf, degrade_trace_queue.front().c_str(), key_size);
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
        memcpy(key_st_addr, key_buf, key_size);
        memcpy(value_st_addr, value_buf, value_size);
        int len = sizeof(KvKeyLen);
        int value = key_size;
        int_to_char(value, key_len_buf, len);
        memcpy(kvkeylen, key_len_buf, sizeof(key_len_buf));
        len = sizeof(KvValueLen);
        value = value_size;
        int_to_char(value, value_len_buf, len);
        memcpy(kvvaluelen, value_len_buf, sizeof(value_len_buf));
        len = sizeof(KvTail);
        value = crc;
        int_to_char(value, crc_buf, len);
        memcpy(kvtail, crc_buf, sizeof(crc_buf));
        kv_info_list_[i].key_len = key_size;
        kv_info_list_[i].value_len = value_size;
        kv_info_list_[i].l_addr  = (void *)input_buf_ptr;
        kv_info_list_[i].lkey = input_buf_mr_->lkey;
        kv_req_ctx_list_[i].kv_all_len = all_len;
        kv_req_ctx_list_[i].kv_info = &kv_info_list_[i];
        if(strcmp(operation_buf, "INSERT") == 0){
            input_buf_ptr += all_len; 
            used_len += all_len;
        } else if(strcmp(operation_buf, "SEARCH") == 0){
            input_buf_ptr += all_len;
            used_len += all_len;
        } else if(strcmp(operation_buf, "UPDATE") == 0){
            int parity_logging_size = all_len + sizeof(ParityLogging);
            input_buf_ptr += all_len + parity_logging_size; 
            used_len += all_len + parity_logging_size;
            load_kv_req_init_update(&kv_req_ctx_list_[i]);
        } else if(strcmp(operation_buf, "DELETE") == 0){
            int parity_logging_size = all_len + sizeof(ParityLogging);
            input_buf_ptr += all_len + parity_logging_size; 
            used_len += all_len + parity_logging_size;
            load_kv_req_init_delete(&kv_req_ctx_list_[i]);
        } else if(strcmp(operation_buf, "DEGRADE") == 0){
            input_buf_ptr += all_len;
            used_len += all_len;
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

int Client::load_kv_req(int num_op, const char * op){
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
    int num_stripe = int((pre_stripe - 1 - base_stripe_id) / num_coroutines_) * num_coroutines_;
    if(strcmp(op, "UPDATE") == 0){
        for(int i = 0;i < num_stripe;i ++){
            int length = update_trace_key_list_per_stripe[i].size();
            for(int j = 0;j < length;j ++){
                update_trace_queue.push(update_trace_key_list_per_stripe[i].front());
                update_trace_key_list_per_stripe[i].pop_front();
            }
        }
        num_total_operations_ = update_trace_queue.size();
        // num_total_operations_ = 1;
    } else if(strcmp(op, "DELETE") == 0){
        for(int i = 0;i < num_stripe;i ++){
            int length = delete_trace_key_list_per_stripe[i].size();
            for(int j = 0;j < length;j ++){
                delete_trace_queue.push(delete_trace_key_list_per_stripe[i].front());
                delete_trace_key_list_per_stripe[i].pop_front();
            }
        }
        num_total_operations_ = delete_trace_queue.size();
        // num_total_operations_ = 1;
    } else if(strcmp(op, "DEGRADE") == 0){
        for(int i = 0;i < num_stripe;i ++){
            for(int j = 0;j < num_op;j ++){
                degrade_trace_queue.push(degrade_read_trace_key_list_per_stripe[i].front());
                degrade_read_trace_key_list_per_stripe[i].pop_front();
            }
        }
        num_total_operations_ = degrade_trace_queue.size();
        // num_total_operations_ = 1;
    } else {
        num_total_operations_ = num_op;
    }
    // num_total_operations_ 
    num_local_operations_ = num_total_operations_;
    RDMA_LOG_IF(3, if_print_log) << "client:" << client_id << " " << "load " << num_local_operations_ << " operations"; 
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
            memcpy(key_buf, degrade_trace_queue.front().c_str(), key_size);
            degrade_trace_queue.pop();
            sprintf(value_buf, "initial-value--%d", i);
        } else {
            convertToBase62(i, key_size, client_id, delimiter_key, key_buf);
            convertToBase62(i, value_size, client_id, delimiter_value, value_buf);
        }
        // record the key and value
        uint32_t all_len = sizeof(KvKeyLen) + sizeof(KvValueLen) + key_size + value_size + sizeof(KvTail);
        KvKeyLen * kvkeylen = (KvKeyLen *)input_buf_ptr;
        KvValueLen * kvvaluelen = (KvValueLen *)(input_buf_ptr + sizeof(KvKeyLen));
        void * key_st_addr = (void *)(input_buf_ptr + sizeof(KvKeyLen) + sizeof(KvValueLen));
        void * value_st_addr = (void *)((uint64_t)key_st_addr + key_size);
        KvTail * kvtail = (KvTail *)((uint64_t)input_buf_ptr + 
        sizeof(KvKeyLen) + sizeof(KvValueLen) + key_size + value_size);
        memcpy(key_st_addr, key_buf, key_size);
        memcpy(value_st_addr, value_buf, value_size);
        int len = sizeof(KvKeyLen);
        int value = key_size;
        int_to_char(value, key_len_buf, len);
        memcpy(kvkeylen, key_len_buf, sizeof(key_len_buf));
        len = sizeof(KvValueLen);
        value = value_size;
        int_to_char(value, value_len_buf, len);
        memcpy(kvvaluelen, value_len_buf, sizeof(value_len_buf));
        len = sizeof(KvTail);
        value = crc;
        int_to_char(value, crc_buf, len);
        memcpy(kvtail, crc_buf, sizeof(crc_buf));
        kv_info_list_[i].key_len = key_size;
        kv_info_list_[i].value_len = value_size;
        kv_info_list_[i].l_addr  = (void *)input_buf_ptr;
        kv_info_list_[i].lkey = input_buf_mr_->lkey;
        kv_req_ctx_list_[i].kv_all_len = all_len;
        kv_req_ctx_list_[i].kv_info = &kv_info_list_[i];
        if(strcmp(operation_buf, "INSERT") == 0){
            input_buf_ptr += all_len; 
            used_len += all_len;
        } else if(strcmp(operation_buf, "SEARCH") == 0){
            input_buf_ptr += all_len;
            used_len += all_len;
        } else if(strcmp(operation_buf, "UPDATE") == 0){
            int parity_logging_size = all_len + sizeof(ParityLogging);
            input_buf_ptr += all_len + parity_logging_size; 
            used_len += all_len + parity_logging_size;
            load_kv_req_init_update(&kv_req_ctx_list_[i]);
        } else if(strcmp(operation_buf, "DELETE") == 0){
            int parity_logging_size = all_len + sizeof(ParityLogging);
            input_buf_ptr += all_len + parity_logging_size; 
            used_len += all_len + parity_logging_size;
            load_kv_req_init_delete(&kv_req_ctx_list_[i]);
        } else if(strcmp(operation_buf, "DEGRADE") == 0){
            input_buf_ptr += all_len;
            used_len += all_len;
            load_kv_req_init_degrade_read(&kv_req_ctx_list_[i]);
        }
        if (used_len >= CLINET_INPUT_BUF_LEN) {
            printf("overflow!\n");
        }
        // record operation
        init_kv_req_ctx(&kv_req_ctx_list_[i], operation_buf);
    }
    print_mes("load finished~");
    return 0;
}

int Client::load_kv_req_from_file_ycsb(const char * fname, uint32_t st_idx, int32_t num_ops){
    RDMA_LOG_IF(3, if_print_log) << "load " << st_idx << " " << num_ops;
    int ret = 0;
    FILE * workload_file = fopen(fname, "r");
    if (workload_file == NULL) {
        RDMA_LOG_IF(2, if_print_log) << "failed to open: " << fname;
        return -1;
    }

    // clear pre struct
    if (num_total_operations_ != 0) {
        delete [] kv_info_list_;
        delete [] kv_req_ctx_list_;
        num_total_operations_ = 0;
        num_local_operations_ = 0;
    }

    int other_size = sizeof(KvKeyLen) + sizeof(KvValueLen) + sizeof(KvTail);
    int pre_value_size = value_size - other_size - key_size;
    if (pre_value_size <= 0) {
        RDMA_LOG_IF(2, if_print_log) << "invalid pre_value_size: " << pre_value_size
                                     << " (value_size=" << value_size << " other_size=" << other_size
                                     << " key_size=" << key_size << ")";
        fclose(workload_file);
        return -1;
    }

    char operation_buf[16];
    char table_buf[16];

    // use a temporary key buffer larger than key_size to safely read tokens
    const int KEY_TMP_MAX = 512;
    char key_tmp[KEY_TMP_MAX];

    // value buffer for building the value payload
    std::vector<char> value_buf_v(pre_value_size);
    char *value_buf = value_buf_v.data();

    // Count total operations safely (bounded read)
    num_total_operations_ = 0;
    rewind(workload_file);
    while (fscanf(workload_file, "%15s %15s %511s", operation_buf, table_buf, key_tmp) == 3) {
        num_total_operations_++;
    }

    if (num_ops == -1) {
        num_local_operations_ = num_total_operations_;
    } else {
        num_local_operations_ = (st_idx + num_ops > num_total_operations_) ? (num_total_operations_ - st_idx) : num_ops;
    }
    print_args("load operations:", num_local_operations_);

    // allocate arrays
    kv_info_list_    = new KVInfo[num_local_operations_];
    kv_req_ctx_list_ = new KVReqCtx[num_local_operations_];
    if (kv_info_list_ == NULL || kv_req_ctx_list_ == NULL) {
        printf("failed to allocate kv_info_list or kv_req_ctx_list\n");
        fclose(workload_file);
        abort();
    }

    // compute per-entry size (fixed layout)
    size_t entry_size = sizeof(KvKeyLen) + sizeof(KvValueLen) + (size_t)key_size + (size_t)pre_value_size + sizeof(KvTail);
    if (entry_size == 0 || entry_size > (size_t)CLINET_INPUT_BUF_LEN) {
        RDMA_LOG_IF(2, if_print_log) << "invalid entry_size: " << entry_size;
        fclose(workload_file);
        return -1;
    }

    uint8_t key_len_buf[sizeof(KvKeyLen)];
    uint8_t value_len_buf[sizeof(KvValueLen)];
    uint8_t crc_buf[sizeof(KvTail)];
    const char* delimiter_value = "-laitini-";

    // lvalue base pointer and used_len (pass by reference to init function)
    uint64_t input_buf_ptr = (uint64_t)input_buf_;
    uint64_t used_len = 0;

    // parse and fill entries
    rewind(workload_file);
    int processed = 0;
    for (int i = 0; i < st_idx + num_local_operations_; ++i) {
        ret = fscanf(workload_file, "%15s %15s %511s", operation_buf, table_buf, key_tmp);
        if (ret != 3) {
            RDMA_LOG_IF(2, if_print_log) << "unexpected/truncated workload line at index " << i << " (fscanf returned " << ret << ")";
            break;
        }
        if (i < st_idx) continue;

        // safe key length
        size_t key_len = strnlen(key_tmp, KEY_TMP_MAX);
        if ((int)key_len > key_size) {
            RDMA_LOG_IF(3, if_print_log) << "truncating key length " << key_len << " to key_size " << key_size;
            key_len = (size_t)key_size;
        }

        // ensure space for this entry
        if (used_len + entry_size > (size_t)CLINET_INPUT_BUF_LEN) {
            RDMA_LOG_IF(2, if_print_log) << "input buffer overflow prevented at op " << i
                                         << " (used_len=" << used_len << ", entry_size=" << entry_size
                                         << ", CLINET_INPUT_BUF_LEN=" << CLINET_INPUT_BUF_LEN << ")";
            fclose(workload_file);
            return -1;
        }

        // compute base pointer for this entry in the input buffer
        uint8_t *entry_ptr = (uint8_t *)input_buf_ + used_len;

        KvKeyLen * kvkeylen = (KvKeyLen *)entry_ptr;
        KvValueLen * kvvaluelen = (KvValueLen *)(entry_ptr + sizeof(KvKeyLen));
        void * key_st_addr = (void *)(entry_ptr + sizeof(KvKeyLen) + sizeof(KvValueLen));
        void * value_st_addr = (void *)((uint8_t *)key_st_addr + key_size);
        KvTail * kvtail = (KvTail *)((uint8_t *)entry_ptr + sizeof(KvKeyLen) + sizeof(KvValueLen) + key_size + pre_value_size);

        // write key area (zero-pad then copy truncated key)
        memset(key_st_addr, 0, (size_t)key_size);
        memcpy(key_st_addr, key_tmp, key_len > (size_t)key_size ? (size_t)key_size : key_len);

        // build value and copy
        convertToBase62(i, pre_value_size, client_id, delimiter_value, value_buf);
        memcpy(value_st_addr, value_buf, (size_t)pre_value_size);

        // fill lengths and crc
        int len = sizeof(KvKeyLen);
        int val = (int)key_len;
        int_to_char(val, key_len_buf, len);
        memcpy(kvkeylen, key_len_buf, sizeof(key_len_buf));

        len = sizeof(KvValueLen);
        val = pre_value_size;
        int_to_char(val, value_len_buf, len);
        memcpy(kvvaluelen, value_len_buf, sizeof(value_len_buf));

        len = sizeof(KvTail);
        val = 0; // crc default 0
        int_to_char(val, crc_buf, len);
        memcpy(kvtail, crc_buf, sizeof(crc_buf));

        // fill kv_info and ctx
        kv_info_list_[processed].key_len = (uint32_t)key_len;
        kv_info_list_[processed].value_len = (uint32_t)pre_value_size;
        kv_info_list_[processed].l_addr  = (void *)entry_ptr;
        kv_info_list_[processed].lkey = input_buf_mr_->lkey;

        kv_req_ctx_list_[processed].kv_all_len = (uint32_t)entry_size;
        kv_req_ctx_list_[processed].kv_info = &kv_info_list_[processed];

        // pass lvalue input_buf_ptr and used_len by reference as required
        init_kv_req_ctx_plus_ycsb(&kv_req_ctx_list_[processed], operation_buf, input_buf_ptr, used_len);

        processed++;
        used_len += entry_size;
    }

    if ((uint32_t)processed < (uint32_t)num_local_operations_) {
        num_local_operations_ = processed;
    }

    print_mes("load finished~");
    fclose(workload_file);
    return 0;
}
/*
int Client::load_kv_req_from_file_ycsb(const char * fname, uint32_t st_idx, int32_t num_ops){
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
        init_kv_req_ctx_plus_ycsb(&kv_req_ctx_list_[i], operation_buf, input_buf_ptr, used_len);
        if(used_len > CLINET_INPUT_BUF_LEN) {
            print_error("overflow~");
        }
    }
    print_mes("load finished~");
    fclose(workload_file);
}
*/

int Client::load_kv_req_file_twitter(const char * fname, uint32_t st_idx, int32_t num_ops){
    print_mes("load kv req~");
    int ret = 0;
    FILE * workload_file = fopen(fname, "r");
    if(workload_file == NULL){
        RDMA_LOG_IF(2, if_print_log) << "failed to open: " << fname;
        return -1;
    }
    print_mes(fname);
    // clear pre struct
    if(num_total_operations_ != 0){
        delete [] kv_info_list_;
        delete [] kv_req_ctx_list_;
        num_total_operations_ = 0;
        num_local_operations_ = 0;
    }
    // need to change
    int key_s, value_s;
    int other_size = KV_KEY_LEN + KV_VALUE_LEN + KV_TAIL_LEN;
    char operation_buf[MAX_OP_SIZE];
    char key_buf[MAX_KEY_SIZE];
    char value_buf[MAX_VALUE_SIZE];
    char key_[4];
    char value_[4];
    unsigned char key_len_buf[2] = {0};
    unsigned char value_len_buf[4] = {0};
    unsigned char crc_buf[1] = {0};
    int crc = 0;
    while (fscanf(workload_file, "%s %s %s %s", operation_buf, key_, value_, key_buf) != EOF) {
        num_total_operations_ ++;
    }
    print_mes("load file op~");
    if (num_ops == -1) {
        num_local_operations_ = num_total_operations_;
    } else {
        num_local_operations_ = (st_idx + num_ops > num_total_operations_) ? num_total_operations_ - st_idx : num_ops;
    }
    print_args("load operations", num_local_operations_);
    kv_info_list_    = new KVInfo[num_local_operations_];
    kv_req_ctx_list_ = new KVReqCtx[num_local_operations_];
    if (kv_info_list_ == NULL || kv_req_ctx_list_ == NULL) {
        printf("failed to allocate kv_info_list or kv_req_ctx_list\n");
        abort();
    }
    uint64_t input_buf_ptr = (uint64_t)input_buf_;
    rewind(workload_file);
    uint64_t used_len = 0;
    uint32_t all_len;
    const char* delimiter_value = "-laitini-";
    for(int i = 0;i < st_idx + num_local_operations_;i ++){
        ret = fscanf(workload_file, "%s %s %s %s", operation_buf, key_, value_, key_buf);
        if(i < st_idx){
            continue;
        }
        key_s = atoi(key_);
        value_s = atoi(value_);
        if(value_s == 0){
            value_s = value_size;
        }
        // generate value
        convertToBase62(i, value_s, client_id, delimiter_value, value_buf);
        // record the key and value
        all_len = other_size + key_s + value_s;
        KvKeyLen * kvkeylen = (KvKeyLen *)input_buf_ptr;
        KvValueLen * kvvaluelen = (KvValueLen *)(input_buf_ptr + KV_KEY_LEN);
        void * key_st_addr = (void *)(input_buf_ptr + KV_KEY_LEN + KV_VALUE_LEN);
        void * value_st_addr = (void *)((uint64_t)key_st_addr + key_s);
        KvTail * kvtail = (KvTail *)((uint64_t)value_st_addr + value_s);
        memcpy(key_st_addr, key_buf, key_s);
        memcpy(value_st_addr, value_buf, value_s);
        int len = KV_KEY_LEN;
        int value = key_s;
        int_to_char(value, key_len_buf, len);
        memcpy(kvkeylen, key_len_buf, len);
        len = KV_VALUE_LEN;
        value = value_s;
        int_to_char(value, value_len_buf, len);
        memcpy(kvvaluelen, value_len_buf, len);
        len = KV_TAIL_LEN;
        value = crc;
        int_to_char(value, crc_buf, len);
        memcpy(kvtail, crc_buf, len);
        kv_info_list_[i].key_len = key_s;
        kv_info_list_[i].value_len = value_s;
        kv_info_list_[i].l_addr  = (void *)input_buf_ptr;
        kv_info_list_[i].lkey = input_buf_mr_->lkey;
        kv_req_ctx_list_[i].kv_all_len = all_len;
        kv_req_ctx_list_[i].kv_info = &kv_info_list_[i];
        init_kv_req_ctx_plus(&kv_req_ctx_list_[i], operation_buf, input_buf_ptr, used_len);
        if(used_len > CLINET_INPUT_BUF_LEN) {
            print_error("overflow~");
        }
    }
    print_mes("load finished~");
    fclose(workload_file);
}

int Client::load_kv_req_file_mncrashed(const char * fname, uint32_t st_idx, int32_t num_ops){
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
        init_kv_req_ctx_plus(&kv_req_ctx_list_[i], operation_buf, input_buf_ptr, used_len);
        if(used_len > CLINET_INPUT_BUF_LEN) {
            print_error("overflow~");
        }
    }
    print_mes("load finished~");
    fclose(workload_file);
}

void Client::get_kv_hash_info(KVInfo * kv_info, __OUT KVHashInfo * hash_info) {
    uint64_t key_addr = (uint64_t)kv_info->l_addr + KV_KEY_LEN + KV_VALUE_LEN;
    hash_info->hash_value = VariableLengthHash((void *)key_addr, kv_info->key_len, 0);
    hash_compute_fp(hash_info);
}

void Client::get_kv_addr_info(KVHashInfo * hash_info, __OUT KVTableAddrInfo * addr_info) {
    get_server_subtable_kv(hash_info, addr_info);
}

void Client::prepare_request(KVReqCtx * ctx) {
    ctx->is_finished = false;
    ctx->ret_val.ret_code = KV_OPS_SUCCESS;
    get_kv_hash_info(ctx->kv_info, &ctx->hash_info);
    get_kv_addr_info(&ctx->hash_info, &ctx->tbl_addr_info);
}

uint64_t Client::connect_parity_key_to_64int(int stripeid, int parityid) {
    uint64_t result = static_cast<uint64_t>(stripeid) << 6;
    result |= static_cast<uint64_t>(parityid & 0x1F);
    return result;
}

uint64_t Client::connect_metadata_key_to_64int(int stripeid, int off) {
    uint64_t result = static_cast<uint64_t>(stripeid) << 6;
    result |= (static_cast<uint64_t>(off & 0x1F) | 0x20);
    return result;
}

void Client::init_encoding_mm_space(){
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
    while(ectx->buf_ == MAP_FAILED){
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
    ectx->metadata_buf = (void *)((uint64_t)ectx->parity_buf + CLIENT_PARITY_BUF);
    ectx->bucket_buf = (void *)((uint64_t)ectx->metadata_buf + CLINET_METADATA_BUF);
    ectx->map_buf = (void *)((uint64_t)ectx->bucket_buf + CLIENT_ENCODING_BUCKET_LEN / 4);
    ectx->parity_chunk = new u8*[k_m];
    ectx->metadata_chunk = new u8*[k_m];
    ectx->mm_alloc_parity_ctx = new ClientMMAllocCtx[m];
    ectx->mm_alloc_metadata_ctx = new ClientMMAllocCtx[k_m];
    for(int i = 0;i < k_m;i ++){
        *(ectx->parity_chunk + i) = (u8 *)((uint64_t)ectx->parity_buf + ectx->block_size * i);
        *(ectx->metadata_chunk + i) = (u8 *)((uint64_t)ectx->metadata_buf + ectx->block_size * i);
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
    ectx->metadata_info = new KVInfo[k_m];
    ectx->metadata_hash_info = new KVHashInfo[k_m];
    ectx->metadata_addr_info = new KVTableAddrInfo;
    ectx->metadata_bucket_buffer = new BucketBuffer[k_m];
    for(int i = 0;i < k_m;i ++){
        ectx->metadata_bucket_buffer[i].kv_modify_pr_cas_list.resize(1);
    }
    ectx->stripe_hash_info = new KVHashInfo;
    ectx->stripe_addr_info = new KVTableAddrInfo;
    ectx->stripe_bucket_buffer = new BucketBuffer[ectx->num_idx_rep];
    for(int i = 0;i < ectx->num_idx_rep;i ++){
        ectx->stripe_bucket_buffer[i].kv_modify_pr_cas_list.resize(1);
    }
    ectx->kv_meta_info = new KVInfo*[k];
    ectx->kv_meta_hash_info = new KVHashInfo*[k];
    ectx->kv_meta_addr_info = new KVTableAddrInfo*[k];
    ectx->kv_meta_bucket_buffer = new BucketBuffer**[k];
    ectx->kv_meta_ctx = new KvEcMetaCtx*[k];
    for(int i = 0;i < k;i ++){
        ectx->kv_meta_info[i] = new KVInfo[ectx->predict_num_kv_meta];
        ectx->kv_meta_hash_info[i] = new KVHashInfo[ectx->predict_num_kv_meta];
        ectx->kv_meta_addr_info[i] = new KVTableAddrInfo[ectx->predict_num_kv_meta];
        ectx->kv_meta_bucket_buffer[i] = new BucketBuffer*[ectx->predict_num_kv_meta];
        ectx->kv_meta_ctx[i] = new KvEcMetaCtx[ectx->predict_num_kv_meta];
        for(int j = 0;j < ectx->predict_num_kv_meta;j ++){
            ectx->kv_meta_bucket_buffer[i][j] = new BucketBuffer[ectx->num_idx_rep];
            for(int t = 0;t < ectx->num_idx_rep;t ++){
                ectx->kv_meta_bucket_buffer[i][j][t].kv_modify_pr_cas_list.resize(1);
            }
        }
    }
    ectx->kv_message = new vector<KvBuffer>[k];
    ectx->en_message = new EncodingMessage[k];
    int kv_meta_buffer_size = ectx->predict_num_kv_meta * k * (ectx->num_idx_rep) * 10;
    ectx->kv_meta_buckets_buffer_addr = new uint64_t[kv_meta_buffer_size];
    for(int i = 0;i < kv_meta_buffer_size;i ++){
        ectx->kv_meta_buckets_buffer_addr[i] = (uint64_t)ectx->map_buf + 
            sizeof(RaceHashBucket) * 2 * i;
    }
    // add log
    ectx->return_faa_kv_log = new uint64_t[num_memory_];
    // init encoding space
    uint64_t ptr = (uint64_t)ectx->bucket_buf;
    int i;
    for(i = 0;i < m;i ++){
        init_bucket_space_big_slot(&ectx->parity_bucket_buffer[i], ptr);        
    }
    for(i = 0;i < k_m;i ++){
        init_bucket_space(&ectx->metadata_bucket_buffer[i], ptr);
    }
    for(i = 0;i < ectx->num_idx_rep;i ++){
        init_bucket_space(&ectx->stripe_bucket_buffer[i], ptr);
    }
    for(i = 0;i < k;i ++){
        for(int j = 0;j < ectx->predict_num_kv_meta;j ++){
            for(int t = 0;t < ectx->num_idx_rep;t ++){
                init_bucket_space(&ectx->kv_meta_bucket_buffer[i][j][t], ptr);
                ectx->kv_meta_bucket_buffer[i][j][t].local_cas_target_value_addr = (void *)ptr;
                ptr += sizeof(uint64_t);
            }
        }
    }
    for(i = 0;i < num_memory_;i ++){
        ectx->return_faa_kv_log[i] = ptr;
        ptr += sizeof(uint64_t);
    }
    ectx->free_log_num = new int[num_memory_];
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

void Client::free_encoding_mm_space(){
    uint64_t buf_size = CLIENT_PARITY_BUF + CLINET_METADATA_BUF + CLIENT_ENCODING_BUCKET_LEN;
    munmap(ectx->buf_, buf_size);
    ibv_dereg_mr(ectx->buf_mr_);
    delete [] ectx->parity_chunk;
    delete [] ectx->metadata_chunk;
    delete [] ectx->mm_alloc_metadata_ctx;
    delete [] ectx->mm_alloc_parity_ctx;
    delete [] ectx->parity_info;
    delete [] ectx->parity_hash_info;
    delete [] ectx->parity_addr_info;
    delete [] ectx->parity_bucket_buffer;
    delete [] ectx->metadata_info;
    delete [] ectx->metadata_hash_info;
    delete [] ectx->metadata_addr_info;
    delete [] ectx->metadata_bucket_buffer;
    delete ectx->stripe_hash_info;
    delete ectx->stripe_addr_info;
    delete [] ectx->stripe_bucket_buffer;
    for(int i = 0;i < k;i ++){
        for(int j = 0;j < ectx->predict_num_kv_meta;j ++){
            delete [] ectx->kv_meta_bucket_buffer[i][j];
        }
        delete [] ectx->kv_meta_info[i];
        delete [] ectx->kv_meta_hash_info[i];
        delete [] ectx->kv_meta_addr_info[i];
        delete [] ectx->kv_meta_bucket_buffer[i];
        delete [] ectx->kv_meta_ctx[i];
    }
    delete [] ectx->kv_meta_info;
    delete [] ectx->kv_meta_hash_info;
    delete [] ectx->kv_meta_addr_info;
    delete [] ectx->kv_meta_bucket_buffer;
    delete [] ectx->kv_meta_ctx;
    delete [] ectx->kv_message;
    delete [] ectx->en_message;
    delete [] ectx->kv_meta_buckets_buffer_addr;
}

void Client::encoding_prepare_async(){
    ectx->data_chunk_id.clear();
    ectx->parity_chunk_id.clear();
    // append core
    stick_this_thread_to_core(encoding_core_id_);
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
    for(int i = 0;i < num_memory_;i ++){
        ectx->free_log_num[i] = 0;
    }
}

void Client::encoding_prepare_sync(){
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
    for(int i = 0;i < num_memory_;i ++){
        ectx->free_log_num[i] = 0;
    }
}

void Client::encoding_make_parity_metadata(){
    KvBuffer *kv_buffer;
    vector<KvBuffer> *kv_list;
    KvKeyLen *kvkeylen;
    KvValueLen *kvvaluelen;
    uint64_t parity_ptr;
    EcMeta * metadata_list;
    bitset<8 * BIT_MAP_SIZE> *bitmap;
    u8 *num_kv_size;
    int kv_align_size;
    int num_subblock;
    int bitmap_offset;
    int num_kv;
    int t;
    int max_meta_size = 0;
    int pre_meta_size;
    int kv_off;
    uint64_t hash_value;
    uint64_t server_id;
    for(int i = 0;i < k;i ++){
        kv_list      = &ectx->kv_message[i];
        num_kv       = kv_list->size();
        parity_ptr   = (uint64_t)ectx->parity_chunk[i];
        bitmap       = (bitset<8 * BIT_MAP_SIZE> *)ectx->metadata_chunk[i];
        num_kv_size  = (u8 *)((uint64_t)ectx->metadata_chunk[i] + BIT_MAP_SIZE);
        metadata_list = (EcMeta *)((uint64_t)ectx->metadata_chunk[i] + BIT_MAP_SIZE + METADATA_LEN_SIZE);
        bitmap_offset = 0;
        int_to_char(num_kv, num_kv_size, METADATA_LEN_SIZE);
        pre_meta_size = BIT_MAP_SIZE + METADATA_LEN_SIZE + sizeof(EcMeta) * num_kv;
        if(max_meta_size < pre_meta_size){
            max_meta_size = pre_meta_size;
        }
        kv_off = 0;
        for(int j = 0;j < num_kv;j ++){
            kv_buffer = &(*kv_list)[j];
            memcpy((void *)parity_ptr, (void *)kv_buffer->kv_addr, kv_buffer->kv_len);
            kv_align_size = get_aligned_size(kv_buffer->kv_len, ectx->kv_alloc_size);
            bitmap->set(bitmap_offset);
            num_subblock = kv_align_size / ectx->kv_alloc_size;
            ectx->kv_meta_ctx[i][j].sid  = ectx->stripe_id;
            ectx->kv_meta_ctx[i][j].fp   = kv_buffer->fp;
            ectx->kv_meta_ctx[i][j].cid  = i;
            ectx->kv_meta_ctx[i][j].off  = bitmap_offset;
            ectx->kv_meta_ctx[i][j].pid1 = ectx->parity_chunk_id[0];
            ectx->kv_meta_ctx[i][j].pid2 = ectx->parity_chunk_id[1];
            for(t = 0;t < kv_buffer->key.length();t ++){
                metadata_list[j].key[t] = kv_buffer->key[t];
            }
            metadata_list[j].key[t] = '\0';
            kvkeylen = (KvKeyLen *)kv_buffer->kv_addr;
            metadata_list[j].key_len = char_to_int(kvkeylen->key_len, sizeof(kvkeylen->key_len));
            // record log read pointer need change
            hash_value = kv_buffer->hash_key;
            server_id = hashring->hash_ringid_map_id(ectx->data_chunk_id[i], hash_value, 1);
            ectx->free_log_num[server_id] ++;
            parity_ptr += kv_align_size;
            bitmap_offset += num_subblock;
        }
    }
    ectx->encoding_metadata_size = max_meta_size;
    ectx->rs_coding->encode_data(ectx->parity_chunk, ectx->block_size);
    ectx->rs_coding->encode_data(ectx->metadata_chunk, ectx->encoding_metadata_size);
    mem_con.parity_data += m * (ectx->block_size + 8);
    mem_con.metadatablock += k_m * (ectx->encoding_metadata_size + 8);
}

void Client::get_server_subtable_parity(KVHashInfo * hash_info, KVTableAddrInfo *addr_info, int buffer_id){
    uint64_t hash_value = hash_info->hash_value;
    get_two_combined_bucket_index(hash_info, addr_info);
    uint64_t r_subtable_off;
    uint16_t target_server_id = hashring->hash_ringid_map_id(buffer_id, hash_value, 0);
    int all_combined_mn = hashring->get_ringid_rep_all_combined_mn(hash_value, buffer_id);
    int second_index = hash_value % all_combined_mn;
    r_subtable_off = hash_index_convert_40_to_64_bits(hashring_table->kv_ec_meta_hash_root_->
        subtable_entry[target_server_id][second_index].subtable_entry_primary.pointer);
    addr_info->server_id_list[0] = target_server_id;
    addr_info->f_bucket_addr[0]  = r_subtable_off + addr_info->f_idx * sizeof(KvEcMetaBucket);
    addr_info->s_bucket_addr[0]  = r_subtable_off + addr_info->s_idx * sizeof(KvEcMetaBucket);
    addr_info->f_bucket_addr_rkey[0] = server_mr_info_map_[target_server_id]->rkey;
    addr_info->s_bucket_addr_rkey[0] = server_mr_info_map_[target_server_id]->rkey;
}

void Client::get_metadata_subtable_addr(uint32_t stripe_id, KVHashInfo *all_hash_info, KVTableAddrInfo *addr_info){
    // get all server id
    std::vector<uint8_t> *num_mn = hashring->get_all_num_mn();
    std::vector<uint8_t> mn_index(k_m);
    uint64_t buffer_id;
    buffer_id = hash_64int_to_64int(stripe_id) % k_m;
    uint64_t hash_value;
    KVHashInfo *hash_info;
    uint8_t server_id, server_index;
    // get server id
    for(int i = 0;i < k_m;i ++){
        hash_info = &all_hash_info[i];
        hash_value = hash_info->hash_value;
        hashring->hash_ringid_map_id_index(buffer_id, hash_value, i, server_id, server_index);
        addr_info->server_id_list[i] = server_id;
        mn_index[(buffer_id + i) % k_m] = server_index;
    }
    // get subtable start addr
    int subtable_index = get_one_dimensional_index(*num_mn, mn_index);
    SubTableEntryMetaData *metadata_subtable = &hashring_table->metadata_hash_root_->subtable_entry[subtable_index];
    // get subtable two combined bucket addr
    uint64_t subtable_start_addr;
    for(int i = 0;i < k_m;i ++){
        subtable_start_addr = hash_index_convert_40_to_64_bits(metadata_subtable->
            subtable_entry_metadata[(buffer_id + i) % k_m].pointer);
        addr_info->f_bucket_addr[i] = subtable_start_addr + addr_info->f_idx * sizeof(RaceHashBucket);
        addr_info->f_bucket_addr_rkey[i] = server_mr_info_map_[addr_info->server_id_list[i]]->rkey;
        addr_info->s_bucket_addr[i] = subtable_start_addr + addr_info->s_idx * sizeof(RaceHashBucket);
        addr_info->s_bucket_addr_rkey[i] = server_mr_info_map_[addr_info->server_id_list[i]]->rkey;
    }
}

void Client::get_server_subtable_metadata(uint32_t stripe_id, KVHashInfo *all_hash_info, KVTableAddrInfo *addr_info){
    get_two_combined_bucket_index(&all_hash_info[0], addr_info);
    get_metadata_subtable_addr(stripe_id, all_hash_info, addr_info);
}

void Client::encoding_prepare_parity(){
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
        get_server_subtable_parity(&ectx->parity_hash_info[i],
            &ectx->parity_addr_info[i], buffer_id);
    }
}

void Client::encoding_parity(){
    encoding_prepare_parity();
    int buffer_id;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    for(int i = 0;i < m;i ++){
        buffer_id = ectx->parity_chunk_id[i];
        mm_->mm_alloc_encoding(buffer->block_size, nm_, ectx->parity_addr_info[i].server_id_list[0],
            &ectx->mm_alloc_parity_ctx[i]);
        if (ectx->mm_alloc_parity_ctx[i].addr_list < server_st_addr_ || 
            ectx->mm_alloc_parity_ctx[i].addr_list >= server_st_addr_ + server_data_len_) {
            print_error("encoding workflow:alloc parity failed~");
            exit(0);
        }
        sr_list = gen_write_kv_sr_lists_encoding(0, &ectx->parity_info[i],
            &ectx->mm_alloc_parity_ctx[i], &sr_list_num, WRITE_PARITY_CHUNK + i);
        send_one_sr_list(sr_list, &ectx->comp_wrid_map);
        sr_list  = gen_bucket_sr_lists_parity(0, &ectx->parity_addr_info[i],
            &ectx->parity_bucket_buffer[i], &sr_list_num, READ_PARITY_CHUNK_BUCKET + i * 2, ectx->buf_mr_->lkey);
        send_one_sr_list(sr_list, &ectx->comp_wrid_map);
    }
}

void Client::encoding_prepare_metadata(){
    uint64_t key, hash_key;
    for(int i = 0;i < k_m;i ++){
        key = connect_metadata_key_to_64int(ectx->stripe_id, i);
        hash_key = hash_64int_to_64int(key);
        ectx->metadata_info[i].l_addr = (void *)ectx->metadata_chunk[i];
        ectx->metadata_info[i].lkey   = ectx->buf_mr_->lkey;
        ectx->metadata_hash_info[i].hash_value = hash_key;
        hash_compute_fp(&ectx->metadata_hash_info[i]);
    }
    get_server_subtable_metadata(ectx->stripe_id, ectx->metadata_hash_info, ectx->metadata_addr_info);
}

void Client::encoding_metadata(){
    encoding_prepare_metadata();
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    for(int i = 0;i < k_m;i ++){
        mm_->mm_alloc_encoding(ectx->encoding_metadata_size, nm_, 
            ectx->metadata_addr_info->server_id_list[i], &ectx->mm_alloc_metadata_ctx[i]);
        sr_list = gen_write_kv_sr_lists_encoding(0, &ectx->metadata_info[i],
            &ectx->mm_alloc_metadata_ctx[i], &sr_list_num, WRITE_METADATA_CHUNK + i);
        send_one_sr_list(sr_list, &ectx->comp_wrid_map);
    }
    sr_list  = gen_bucket_sr_lists_metadata(0, ectx->metadata_addr_info,
        ectx->metadata_bucket_buffer, &sr_list_num, READ_METADATA_BUCKET,
        ectx->buf_mr_->lkey);
    send_one_sr_list(sr_list, &ectx->comp_wrid_map);
}

void Client::prepare_stripe_meta(int stripe_id, KVHashInfo *hash_info, KVTableAddrInfo *addr_info){
    hash_info->hash_value = hash_64int_to_64int(stripe_id);
    hash_compute_fp(hash_info);
    get_server_subtable_stripe_meta(hash_info, addr_info);
}

void Client::encoding_prepare_stripe(){
    prepare_stripe_meta(ectx->stripe_id, ectx->stripe_hash_info, ectx->stripe_addr_info);
}

void Client::free_sr_list(IbvSrList * sr_list, int num_sr_lists){
    for(int i = 0;i < num_sr_lists;i ++){
        free(sr_list[i].sr_list[0].sg_list);
        free(sr_list[i].sr_list);
    }
    free(sr_list);
}

void Client::encoding_stripe(){
    encoding_prepare_stripe();
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_bucket_sr_lists_metadata(0, ectx->stripe_addr_info, &ectx->stripe_bucket_buffer[0],
        &sr_list_num, READ_STRIPE_META_BUCKET, ectx->buf_mr_->lkey);
    send_one_sr_list(sr_list, &ectx->comp_wrid_map);
}
    
void Client::encoding_prepare_kv_meta(){
    vector<KvBuffer> *kv_list;
    KvBuffer *kv_buffer;
    KVTableAddrInfo *addr_info;
    KvEcMetaMes  *kv_ec_meta_mes;
    for(int i = 0;i < k;i ++){
        kv_list = &ectx->kv_message[i];
        for(int j = 0;j < kv_list->size();j ++){
            kv_buffer = &(*kv_list)[j];
            addr_info = &ectx->kv_meta_addr_info[i][j];
            for(int k = 0;k < m + 1;k ++){
                kv_ec_meta_mes = &kv_buffer->kv_mes;
                addr_info->server_id_list[k]     = kv_ec_meta_mes->server_id[k];
                addr_info->f_bucket_addr[k]      = kv_ec_meta_mes->slot_addr[k];
                addr_info->f_bucket_addr_rkey[k] = server_mr_info_map_[addr_info->server_id_list[k]]->rkey;
            }
        }
    }
}

void Client::encoding_kv_meta(){
    encoding_prepare_kv_meta();
}

void Client::encoding_init_sr_list(){
    ectx->sr_list_batch.clear();
    ectx->sr_list_num_batch.clear();
    ectx->comp_wrid_map.clear();
}

void Client::encoding_write_and_read_buckets_async(){
    encoding_init_sr_list();
    encoding_parity();
    encoding_metadata();
    encoding_stripe();
    encoding_kv_meta();
    poll_completion_thread(ectx->comp_wrid_map);
}

void Client::encoding_write_and_read_buckets_sync(){
    int ret;
    encoding_init_sr_list();
    encoding_parity();
    encoding_metadata();
    encoding_stripe();
    encoding_kv_meta();
    ret = nm_->rdma_post_check_sr_list(&ectx->comp_wrid_map);
    if(ret == -1){
        RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " << "encoding workflow: make stripe: [" << ectx->stripe_id
            << "] write and read bucket RTT send sr list failed";
        exit(0);
    }
}

void Client::encoding_cas_parity(){
    RaceHashSlot * new_slot;
    BucketBuffer * bucketbuffer;
    KVTableAddrInfo *addr_info;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    uint64_t local_com_bucket_addr, old_local_slot_addr, remote_slot_addr;
    int bucket_idx, slot_idx;
    uint64_t offset;
    for(int i = 0;i < m;i ++){
        bucketbuffer = &ectx->parity_bucket_buffer[i];
        addr_info    = &ectx->parity_addr_info[i];
        get_bucket_info_big(bucketbuffer);
        find_empty_slot_from_bucket_buffer_big(bucketbuffer, addr_info);
        bucket_idx = bucketbuffer->bucket_idx;
        slot_idx   = bucketbuffer->slot_idx;
        if(bucket_idx == -1){
            print_error("encoding workflow:find parity slot failed~");
            exit(0);
        }
        if(bucket_idx < 2){
            offset = sizeof(KvEcMetaBucket) * bucket_idx + sizeof(uint64_t) * 2 + sizeof(KvEcMetaSlot) * slot_idx;
            local_com_bucket_addr = (uint64_t)bucketbuffer->f_com_bucket;
            old_local_slot_addr   = local_com_bucket_addr + offset;
            remote_slot_addr = addr_info->f_bucket_addr[0] + offset;
        } else {
            offset = sizeof(KvEcMetaBucket) * (bucket_idx - 2) + sizeof(uint64_t) * 2 + sizeof(KvEcMetaSlot) * slot_idx;
            local_com_bucket_addr = (uint64_t)bucketbuffer->s_com_bucket;
            old_local_slot_addr   = local_com_bucket_addr + offset;
            remote_slot_addr = addr_info->s_bucket_addr[0] + offset;
        }
        uint64_t old_value = *(uint64_t *)old_local_slot_addr;
        RaceHashSlot *old_slot = (RaceHashSlot *)old_local_slot_addr;
        fill_slot(&ectx->mm_alloc_parity_ctx[i], &ectx->parity_hash_info[i], old_slot);
        old_slot->kv_len = 0;
        uint64_t new_value = *(uint64_t *)old_local_slot_addr;
        fill_cas_slot_through_bucket_buffer_value(
            &bucketbuffer->kv_modify_pr_cas_list[0],
            addr_info->server_id_list[0], ectx->buf_mr_->lkey,
            remote_slot_addr, 
            (uint64_t)bucketbuffer->local_cas_return_value_addr,
            old_value, new_value
        );
        sr_list = gen_cas_sr_lists_encoding(0, bucketbuffer->kv_modify_pr_cas_list,
            &sr_list_num, CAS_PARITY_BUCKET + i);
        send_one_sr_list(sr_list, &ectx->comp_wrid_map);
    }
}

void Client::encoding_cas_metadata(){
    RaceHashSlot * new_slot;
    BucketBuffer * bucketbuffer;
    KVTableAddrInfo *addr_info;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    uint64_t local_com_bucket_addr, old_local_slot_addr, remote_slot_addr;
    int bucket_idx, slot_idx;
    addr_info    = ectx->metadata_addr_info;
    for(int i = 0;i < k_m;i ++){
        bucketbuffer = &ectx->metadata_bucket_buffer[i];
        get_bucket_info(bucketbuffer);
        if(i == 0){
            find_empty_slot_from_bucket_buffer(bucketbuffer, addr_info);
            bucket_idx = bucketbuffer->bucket_idx;
            slot_idx   = bucketbuffer->slot_idx;
        }
        if(bucket_idx == -1){
            print_error("encoding workflow:find metadata slot failed~");
            exit(0);
        }
        if(bucket_idx < 2){
            local_com_bucket_addr = (uint64_t)bucketbuffer->f_com_bucket;
            old_local_slot_addr   = (uint64_t)&bucketbuffer->
                f_com_bucket[bucket_idx].slots[slot_idx];
            remote_slot_addr = addr_info->f_bucket_addr[i] + 
                (old_local_slot_addr - local_com_bucket_addr);
        } else {
            local_com_bucket_addr = (uint64_t)bucketbuffer->s_com_bucket;
            old_local_slot_addr   = (uint64_t)&bucketbuffer->
                s_com_bucket[bucket_idx - 2].slots[slot_idx];
            remote_slot_addr = addr_info->s_bucket_addr[i] + 
                (old_local_slot_addr - local_com_bucket_addr);
        }
        RaceHashSlot *old_slot = (RaceHashSlot *)old_local_slot_addr;
        fill_slot(&ectx->mm_alloc_metadata_ctx[i], &ectx->metadata_hash_info[i], old_slot);
        uint64_t new_value = *(uint64_t *)old_local_slot_addr;
        fill_cas_slot_through_bucket_buffer_value(
            &bucketbuffer->kv_modify_pr_cas_list[0],
            addr_info->server_id_list[i], ectx->buf_mr_->lkey,
            remote_slot_addr, 
            (uint64_t)bucketbuffer->local_cas_return_value_addr,
            0, new_value
        );
        sr_list = gen_cas_sr_lists_encoding(0, bucketbuffer->kv_modify_pr_cas_list,
            &sr_list_num, CAS_METADATA_BUCKET + i);
        send_one_sr_list(sr_list, &ectx->comp_wrid_map);
    }
}

void Client::encoding_cas_stripe(){
    RaceHashSlot * new_slot;
    BucketBuffer * bucketbuffer;
    KVTableAddrInfo *addr_info;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    uint64_t local_com_bucket_addr, old_local_slot_addr, remote_slot_addr;
    int bucket_idx, slot_idx;
    StripeMetaSlot slot;
    fill_stripe_meta_slot(&slot, &ectx->stripe_meta_ctx);
    uint64_t new_value = *(uint64_t *)&slot;
    uint64_t old_value = 0;
    addr_info    = ectx->stripe_addr_info;
    for(int i = 0;i < ectx->num_idx_rep;i ++){
        bucketbuffer = &ectx->stripe_bucket_buffer[i];
        get_bucket_info(bucketbuffer);
        if(i == 0){
            find_empty_slot_from_bucket_buffer(bucketbuffer, addr_info);
            bucket_idx = bucketbuffer->bucket_idx;
            slot_idx   = bucketbuffer->slot_idx;
        }
        if(bucket_idx == -1){
            print_error("encoding workflow:find stripe meta slot failed~");
            continue;
        }
        if(bucket_idx < 2){
            local_com_bucket_addr = (uint64_t)bucketbuffer->f_com_bucket;
            old_local_slot_addr   = (uint64_t)&bucketbuffer->
                f_com_bucket[bucket_idx].slots[slot_idx];
            remote_slot_addr = addr_info->f_bucket_addr[i] + 
                (old_local_slot_addr - local_com_bucket_addr);
        } else {
            local_com_bucket_addr = (uint64_t)bucketbuffer->s_com_bucket;
            old_local_slot_addr   = (uint64_t)&bucketbuffer->
                s_com_bucket[bucket_idx - 2].slots[slot_idx];
            remote_slot_addr = addr_info->s_bucket_addr[i] + 
                (old_local_slot_addr - local_com_bucket_addr);
        }
        fill_cas_slot_through_bucket_buffer_value(
            &bucketbuffer->kv_modify_pr_cas_list[0],
            addr_info->server_id_list[i], ectx->buf_mr_->lkey,
            remote_slot_addr, 
            (uint64_t)bucketbuffer->local_cas_return_value_addr,
            old_value, new_value
        );
        sr_list = gen_cas_sr_lists_encoding(0, bucketbuffer->kv_modify_pr_cas_list,
            &sr_list_num, CAS_STRIPE_BUCKET + i);
        send_one_sr_list(sr_list, &ectx->comp_wrid_map);
    }
}

IbvSrList *Client::gen_one_sr_list_encoding(uint64_t local_addr, uint32_t local_len, uint32_t lkey, 
    uint32_t coro_id, uint8_t server_id, uint32_t type, uint32_t wr_id_offset, 
    ibv_wr_opcode opcode, uint64_t remote_addr, int send_flags, ibv_send_wr *next){
    IbvSrList   *sr_list = get_one_sr_list_encoding();
    struct ibv_send_wr * sr = get_one_sr_encoding();
    struct ibv_sge     *sge = get_one_sge_encoding();
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
    sr_list->num_sr = 1;
    sr_list->server_id = server_id;
    sr_list->sr_list = sr;
    return sr_list;
}

void Client::encoding_write_kv_meta(){
    RaceHashSlot * new_slot;
    BucketBuffer * bucketbuffer;
    KVTableAddrInfo *addr_info;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    uint64_t local_com_bucket_addr, old_local_slot_addr, remote_slot_addr;
    int bucket_idx, slot_idx;
    vector<KvBuffer> *kv_list;
    uint64_t new_value;
    uint64_t old_value = 0;
    KvMetaSlot *old_kv_meta_slot;
    EcMetaSlot *old_ec_meta_slot;
    KvBuffer *kv_buffer;
    for(int i = 0;i < k;i ++){  
        kv_list = &ectx->kv_message[i];
        for(int j = 0;j < kv_list->size();j ++){
            kv_buffer = &(*kv_list)[j];
            addr_info = &ectx->kv_meta_addr_info[i][j];
            for(int t = 0;t < ectx->num_idx_rep;t ++){
                bucketbuffer = &ectx->kv_meta_bucket_buffer[i][j][t];
                remote_slot_addr = kv_buffer->kv_mes.slot_addr[t];
                if(t == 0){
                    // fill primary kv ec meta
                    old_ec_meta_slot = (EcMetaSlot *)bucketbuffer->local_cas_target_value_addr;
                    fill_ec_meta_slot(old_ec_meta_slot, &ectx->kv_meta_ctx[i][j]);
                    new_value = *(uint64_t *)old_ec_meta_slot;
                } else {
                    // fill backup kv ec meta
                    old_kv_meta_slot = (KvMetaSlot *)bucketbuffer->local_cas_target_value_addr;
                    fill_kv_meta_slot(old_kv_meta_slot, &ectx->kv_meta_ctx[i][j]);
                    new_value = *(uint64_t *)old_kv_meta_slot;
                }
                sr_list = gen_one_sr_list_encoding((uint64_t)bucketbuffer->local_cas_target_value_addr,
                    sizeof(uint64_t), ectx->buf_mr_->lkey, 0, addr_info->server_id_list[t],
                    WRITE_KV_SLOT, (i * ectx->predict_num_kv_meta + j) * ectx->num_idx_rep + t,
                    IBV_WR_RDMA_WRITE, remote_slot_addr, 0, NULL);
                ectx->sr_list_batch.push_back(sr_list);
                ectx->sr_list_num_batch.push_back(1);
            }
        }
        nm_->rdma_post_sr_list_batch_sync_send(ectx->sr_list_batch,
            ectx->sr_list_num_batch, ectx->comp_wrid_map);
        ectx->sr_list_batch.clear();
        ectx->sr_list_num_batch.clear();
    }
}

void Client::encoding_cas_check(){

    uint64_t return_value, origin_value;

    // for(int i = 0;i < m;i ++){
    //     return_value = *(uint64_t *)ectx->parity_bucket_buffer[i].
    //         kv_modify_pr_cas_list[0].l_kv_addr;
    //     origin_value = ectx->parity_bucket_buffer[i].kv_modify_pr_cas_list[0].orig_value;
    //     if(return_value != origin_value){
    //         RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "encoding workflow:cas parity slot failed";
    //         RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "after  cas value is:" << return_value;
    //         RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "before cas value is:" << origin_value;
    //         RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "stripe id is:" << ectx->stripe_id;
    //         RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "num parity:" << i;
    //         // exit(0);
    //     }
    // }

    for(int i = 0;i < k_m;i ++){
        return_value = *(uint64_t *)ectx->metadata_bucket_buffer[i].
            kv_modify_pr_cas_list[0].l_kv_addr;
        origin_value = ectx->metadata_bucket_buffer[i].kv_modify_pr_cas_list[0].orig_value;
        if (return_value != origin_value) {
            RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "encoding workflow:cas metadata slot failed";
            RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "after  cas value is:" << return_value;
            RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "before cas value is:" << origin_value;
            RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "stripe id is:" << ectx->stripe_id;
            RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "num metadata:" << i;
        }
    }

    for(int i = 0;i < ectx->num_idx_rep;i ++){
        return_value = *(uint64_t *)ectx->stripe_bucket_buffer[i].
            kv_modify_pr_cas_list[0].l_kv_addr;
        origin_value = ectx->stripe_bucket_buffer[i].kv_modify_pr_cas_list[0].orig_value;
        if (return_value != origin_value) {
            RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "encoding workflow:cas stripe meta failed";
            RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "after  cas value is:" << return_value;
            RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "before cas value is:" << origin_value;
            RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "stripe id is:" << ectx->stripe_id;
            RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << "rep stripe:" << i;
        }
    }
}

void Client::encoding_cas_async(){
    int ret;
    encoding_init_sr_list();
    encoding_cas_parity();
    encoding_cas_metadata();
    encoding_cas_stripe();
    encoding_write_kv_meta();
    ret = poll_completion_thread(ectx->comp_wrid_map);
    if(ret == -1){
        print_error("encoding workflow:cas failed~");
        exit(0);
    }
    encoding_cas_check();
}

void Client::encoding_cas_sync(){
    int ret;
    encoding_init_sr_list();
    encoding_cas_parity();
    encoding_cas_metadata();
    encoding_cas_stripe();
    encoding_write_kv_meta();
    ret = nm_->rdma_post_check_sr_list(&ectx->comp_wrid_map);
    if(ret == -1){
        print_error("encoding workflow:cas failed~");
        exit(0);
    }
    encoding_cas_check();
}

void Client::make_trace(){
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

void Client::encoding_check(){
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

void Client::encoding_check_make_trace(){
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

void Client::encoding_wait(){
    encoding_thread.waitDone();
}

void Client::print_encoding_mes(){
    buffer->print_encoding_mes();
}

void Client::print_buffer_mes(){
    buffer->print_buffer_mes();
}

void Client::print_buffer_all_mes(){
    buffer->print_mes();
}

void Client::encoding_leave(){
    bool if_no_encoding = false;
    do{
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
                ectx->if_encoding = true;
                encoding_thread.exec(std::bind(&Client::encoding_and_make_stripe_async, this));
            } else {
                if_no_encoding = true;
            }
        }
    } while(if_no_encoding == false);
}

void Client::encoding_leave_make_trace(){
    bool if_no_encoding = false;
    do{
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
                ectx->if_encoding = true;
                encoding_thread.exec(std::bind(&Client::encoding_and_make_stripe_async_trace, this));
            } else {
                if_no_encoding = true;
            }
        }
    } while(if_no_encoding == false);
}

void Client::encoding_check_async(){
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
            ectx->if_encoding = true;
            encoding_thread.exec(std::bind(&Client::encoding_and_make_stripe_async, this));
        }
    }
}

void Client::init_trace(){
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

void Client::encoding_check_async_make_trace(){
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
            ectx->if_encoding = true;
            encoding_thread.exec(std::bind(&Client::encoding_and_make_stripe_async_trace, this));
        }
    }
}

void Client::encoding_finish(){
    ectx->if_encoding = false;
}

void Client::encoding_finish_trace(){
    make_trace();
    ectx->if_encoding = false;
}

void Client::encoding_and_make_stripe_async(){
    encoding_prepare_async();
    encoding_make_parity_metadata();
    encoding_write_and_read_buckets_async();
    encoding_cas_async();
    encoding_faa_log_async();
    encoding_finish();
}

void Client::encoding_and_make_stripe_async_trace(){
    encoding_prepare_async();
    encoding_make_parity_metadata();
    encoding_write_and_read_buckets_async();
    encoding_cas_async();
    encoding_faa_log_async();
    encoding_finish_trace();
}

void Client::encoding_faa_log_sync(){
    encoding_init_sr_list();
    IbvSrList *sr_list;
    int all_num_kv = 0;
    for(int i = 0;i < num_memory_;i ++){
        if(ectx->free_log_num[i] != 0){
            all_num_kv += ectx->free_log_num[i];
            sr_list = gen_faa_log(num_coroutines_, i, false, ectx->free_log_num[i], 
                ectx->return_faa_kv_log[i], ectx->buf_mr_->lkey, 0);
            client_log->log_list[i].rp += ectx->free_log_num[i];
            send_one_sr_list(sr_list, &ectx->comp_wrid_map);
        }
    }
    mem_con.kv_ec_meta += (m + 1) * all_num_kv * 8;
    mem_con.stripe_meta += (m + 1) * 8;
    int ret = nm_->rdma_post_check_sr_list(&ectx->comp_wrid_map);
    if(ret == -1){
        print_error("encoding workflow:faa log failed");
        exit(0);
    }
}

void Client::encoding_faa_log_async(){
    encoding_init_sr_list();
    IbvSrList *sr_list;
    for(int i = 0;i < num_memory_;i ++){
        if(ectx->free_log_num[i] != 0){
            sr_list = gen_faa_log_encoding(num_coroutines_, i, false, ectx->free_log_num[i], 
                ectx->return_faa_kv_log[i], ectx->buf_mr_->lkey, 0);
            client_log->log_list[i].rp += ectx->free_log_num[i];
            send_one_sr_list(sr_list, &ectx->comp_wrid_map);
        }
    }
    int ret = poll_completion(ectx->comp_wrid_map);
    if(ret == -1){
        print_error("encoding workflow:faa log failed");
        exit(0);
    }
}

void Client::encoding_and_make_stripe_sync(){
    encoding_prepare_sync();
    encoding_make_parity_metadata();
    encoding_write_and_read_buckets_sync();
    encoding_cas_sync();
    encoding_faa_log_sync();
    encoding_finish();
}

void Client::encoding_and_make_stripe_sync_trace(){
    encoding_prepare_sync();
    encoding_make_parity_metadata();
    encoding_write_and_read_buckets_sync();
    encoding_cas_sync();
    encoding_faa_log_sync();
    encoding_finish_trace();
}

int Client::kv_insert(KVReqCtx * ctx) { 
    prepare_request(ctx);
    kv_insert_read_buckets_and_write_kv(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    kv_insert_write_buckets(ctx);
    if(ctx->is_finished){
        return ctx->ret_val.ret_code;
    }
    kv_insert_buffer(ctx);
    return 0;
}

int Client::kv_insert_only_buffer(KVReqCtx * ctx){
    kv_insert_buffer(ctx);
}

int Client::kv_insert_posibility_crash_sync(KVReqCtx * ctx){
    kv_insert_read_buckets_and_write_kv_sync(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    kv_insert_write_buckets_sync_crash(ctx);
    kv_insert_buffer(ctx);
    return ctx->ret_val.ret_code;
}

int Client::kv_insert_sync(KVReqCtx * ctx) { 
    prepare_request(ctx);
    kv_insert_read_buckets_and_write_kv_sync(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    kv_insert_write_buckets_sync(ctx);
    kv_insert_buffer(ctx);
    mem_con.kv_data += ctx->kv_all_len + 8;
    return ctx->ret_val.ret_code;
}

int Client::kv_insert_sync_batch(KVReqCtx * ctx) { 
    prepare_request(ctx);
    kv_insert_read_buckets_and_write_kv_sync(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    kv_insert_write_buckets_sync_batch(ctx);
    kv_insert_buffer(ctx);
    mem_con.kv_data += ctx->kv_all_len + 8;
    return ctx->ret_val.ret_code;
}

int Client::kv_insert_crash_sync(KVReqCtx * ctx) { 
    kv_insert_read_buckets_and_write_kv_sync(ctx);
    return ctx->ret_val.ret_code;
}

void Client::kv_insert_buffer(KVReqCtx * ctx){
    KvEcMetaMes kv_mes;
    uint64_t offset;
    for(int i = 0;i < m + 1;i ++){
        offset = ctx->bucket_idx * sizeof(RaceHashBucket) + sizeof(uint64_t) + ctx->slot_idx * sizeof(RaceHashSlot);
        kv_mes.server_id[i] = ctx->tbl_addr_info.server_id_list[i];
        kv_mes.slot_addr[i] = ctx->tbl_addr_info.f_bucket_addr[i] + (i == 0 ? offset * 2 + sizeof(RaceHashSlot) : offset);
    }
    buffer->insert(ctx->hash_info.hash_value % k_m, ctx->key_str, 
        (uint64_t)ctx->kv_info->l_addr,
        ctx->kv_all_len, ctx->hash_info.hash_value, kv_mes, ctx->hash_info.fp);
}

void * Client::kv_search_sync(KVReqCtx * ctx) { 
    prepare_request(ctx);
    kv_search_read_buckets_sync(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.value_addr;
    }
    kv_search_read_kv_sync(ctx); 
    kv_search_check_kv(ctx);
    return ctx->ret_val.value_addr;
}

void * Client::kv_search_sync_batch(KVReqCtx * ctx) { 
    prepare_request(ctx);
    kv_search_read_buckets_sync_batch(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.value_addr;
    }
    kv_search_read_kv_sync_batch(ctx); 
    kv_search_check_kv(ctx);
    return ctx->ret_val.value_addr;
}

void * Client::kv_search(KVReqCtx * ctx) { 
    kv_search_read_buckets(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.value_addr;
    }
    kv_search_read_kv(ctx); 
    if(ctx->is_finished){
        return ctx->ret_val.value_addr;
    }
    kv_search_check_kv(ctx);
    return ctx->ret_val.value_addr;
}

void Client::print_error(const char * err){
    RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << " " << err;
}

void Client::print_mes(const char *mes){
    RDMA_LOG_IF(3, if_print_log) << "client:" << client_id << " " << mes;
}

void Client::print_sp(const char *sp){
    RDMA_LOG_IF(4, if_print_log) << "client:" << client_id << " " << sp;
}

void Client::print_args(const char *args_name, double value){
    RDMA_LOG_IF(3, if_print_log) << "client:" << client_id << " " << args_name << ": " << value;
}

void Client::kv_update_analysis_stripe_meta(UpdateCtx *uctx){
    uctx->is_finished = false;
    get_bucket_info(&uctx->stripe_meta_bucket_info);
    bool if_find = false;
    if_find = find_stripe_meta_slot_struct(&uctx->stripe_meta_bucket_info,
        &uctx->stripe_meta_ctx, uctx->kv_meta_ctx.sid, 
        &uctx->stripe_meta_idx);
    if(if_find == false){
        // print_error("update workflow:analysis stripe meta bucket:find slot failed~");
        uctx->is_finished = true;
        uctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        code_num_count.update_RTT2_read_stripe_meta_failed ++;
        return;
    } 
    if(uctx->stripe_meta_ctx.WL == pow(2, 5) - 1){
        // print_error("update workflow:analysis stripe meta bucket:find WL is full~");
        // RDMA_LOG_IF(2, if_print_log) << "client:" << client_id << 
        //     "update workflow:analysis stripe meta bucket:find WL is full~" <<
        //     "stripe id:" << uctx->stripe_meta_ctx.stripe_id;
        uctx->is_finished = true;
        uctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        code_num_count.update_RTT2_read_wl_is_15 ++;
        return;
    }
    if(uctx->stripe_meta_ctx.WL == 0){
        uctx->is_cas = true;
    } else {
        uctx->is_cas = false;
    }
    if(uctx->stripe_meta_ctx.RL != 0){
        print_error("update workflow:analysis \
            stripe meta bucket:find RL is not 0(there is degrade read doing)~");
        uctx->is_finished = true;
        uctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    for(int i = 0;i < m - 1;i ++){
        uctx->parity_id[i + 1] = uctx->stripe_meta_ctx.pid[i];
    }
}

void Client::kv_update_make_cas_stripe_meta(UpdateCtx *uctx){
    int bucket_idx = uctx->stripe_meta_idx.bucket_idx;
    int slot_idx   = uctx->stripe_meta_idx.slot_idx;
    uint64_t local_com_bucket_addr, old_local_slot_addr, remote_slot_addr;
    if(bucket_idx < 2){
        local_com_bucket_addr = (uint64_t)uctx->stripe_meta_bucket_info.f_com_bucket;
        old_local_slot_addr = (uint64_t)&uctx->stripe_meta_bucket_info.
            f_com_bucket[bucket_idx].slots[slot_idx];
        remote_slot_addr = uctx->stripe_meta_addr_info->f_bucket_addr[0] +
            (old_local_slot_addr - local_com_bucket_addr);
    } else {
        local_com_bucket_addr = (uint64_t)uctx->stripe_meta_bucket_info.s_com_bucket;
        old_local_slot_addr = (uint64_t)&uctx->stripe_meta_bucket_info.
            s_com_bucket[bucket_idx - 2].slots[slot_idx];
        remote_slot_addr = uctx->stripe_meta_addr_info->s_bucket_addr[0] +
            (old_local_slot_addr - local_com_bucket_addr);
    }
    uint64_t old_value = *(uint64_t *)old_local_slot_addr;
    StripeMetaSlot *stripe_meta = (StripeMetaSlot *)old_local_slot_addr;
    write_stripe_meta_wl(stripe_meta, 1);
    uint64_t new_value = *(uint64_t *)old_local_slot_addr;
    fill_cas_slot_through_bucket_buffer_value(
        &uctx->stripe_meta_bucket_info.kv_modify_pr_cas_list[0],
        uctx->stripe_meta_addr_info->server_id_list[0],
        local_buf_mr_->lkey, remote_slot_addr,
        (uint64_t)uctx->stripe_meta_bucket_info.local_cas_return_value_addr,
        old_value, new_value
    );
}

void Client::kv_update_cas_stripe_meta(KVReqCtx *ctx, bool if_add){
    UpdateCtx *uctx = ctx->uctx;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    if(if_add == false){
        sr_list = gen_faa_sr_lists(uctx->coro_id,
            uctx->stripe_meta_bucket_info.kv_modify_pr_cas_list,
            &sr_list_num, -faa_need_value_WL);
        if(if_req_latency){
            ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_FAA_STRIPE_META;
        }
        kv_update_send(uctx, sr_list, sr_list_num);
        return;
    }
    kv_update_make_cas_stripe_meta(uctx);
    if(uctx->is_cas == true){
        sr_list = gen_cas_sr_lists(uctx->coro_id,
            uctx->stripe_meta_bucket_info.kv_modify_pr_cas_list,
            &sr_list_num);
    } else {
        sr_list = gen_faa_sr_lists(uctx->coro_id,
            uctx->stripe_meta_bucket_info.kv_modify_pr_cas_list,
            &sr_list_num, faa_need_value_WL);
    }
    if(if_req_latency){
        ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_CAS_STRIPE_META;
    }
    kv_update_send(uctx, sr_list, sr_list_num);
    return;
}

IbvSrList *Client::gen_sr_list(uint64_t local_addr, uint32_t local_len, uint32_t lkey, 
    uint32_t coro_id, uint8_t server_id, uint32_t type, uint32_t wr_id_offset, 
    ibv_wr_opcode opcode, uint64_t remote_addr){
    IbvSrList * ret_sr_list = get_one_sr_list();
    struct ibv_send_wr * sr = get_one_sr();
    struct ibv_sge     * sge = get_one_sge();
    sge->addr = local_addr;
    sge->length = local_len;
    sge->lkey = lkey;
    sr->wr_id = ib_gen_wr_id(coro_id, server_id, type, wr_id_offset);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode = opcode;
    sr->wr.rdma.remote_addr = remote_addr;
    sr->wr.rdma.rkey = server_mr_info_map_[server_id]->rkey;
    sr->send_flags = 0;
    sr->next = NULL;
    ret_sr_list->sr_list = sr;
    ret_sr_list->num_sr = 1;
    ret_sr_list->server_id = server_id;
    return ret_sr_list;
}

void Client::log_entry_rp_range(uint8_t server_id){
    client_log->log_list[server_id].rp = 
        (client_log->log_list[server_id].rp + 1) % MAX_LOG_ENTRY;
}

uint64_t Client::get_log_entry_addr(uint8_t server_id){
    uint64_t ret = client_log->log_list[server_id].remote_log_entry_addr + 
        client_log->log_list[server_id].wp * LOG_ENTRY_LEN;
    client_log->log_list[server_id].wp = 
        (client_log->log_list[server_id].wp + 1) % MAX_LOG_ENTRY;
    return ret;
}

void Client::kv_update_write_stripe_meta_log(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    uint8_t log_type;
    uint32_t wr_id_type;
    if(uctx->is_cas == true){
        log_type = LOG_UP_DE_CAS_STRIPE_META;
        wr_id_type = UPDATE_CAS_STRIPE_META_LOG;
    } else {
        log_type = LOG_UP_DE_FAA_STRIPE_META;
        wr_id_type = UPDATE_FAA_STRIPE_META_LOG;
    }
    uint8_t server_id = uctx->stripe_meta_addr_info->server_id_list[0];
    make_log_entry((LogEntry *)uctx->stripe_meta_log_buf_addr, log_type, 
        uctx->stripe_meta_idx.slot_value);
    IbvSrList * sr_list;
    sr_list = gen_sr_list(uctx->stripe_meta_log_buf_addr,
        LOG_ENTRY_LEN, local_buf_mr_->lkey, uctx->coro_id, server_id, wr_id_type,
        0, IBV_WR_RDMA_WRITE, get_log_entry_addr(server_id));
    if(if_req_latency){
        ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_WRITE_STRIPE_META_LOG;
    }
    kv_update_send(uctx, sr_list, 1);
}

void Client::kv_update_cas_stripe_meta_first(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_WRITE_STRIPE_META_LOG);
        ctx->req_timer->start_timer(UPDATE_CAS_STRIPE_META);
    }
    kv_update_analysis_stripe_meta(uctx);
    if(uctx->is_finished == true){
        // print_error("analysis stripe meta failed~");
        return;
    }
    // kv_update_write_stripe_meta_log(ctx);
    kv_update_cas_stripe_meta(ctx, true);
}

void Client::kv_update_prepare_PL_info(KVReqCtx * ctx){
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_ENCODING_PARITY);
    }
    // prepare encoding A * (D2 - D1)
    u8 *D2 = (u8 *)ctx->kv_info->l_addr;
    u8 *D1 = (u8 *)ctx->kv_read_addr_list[ctx->uctx->PL_match_idx].l_kv_addr;
    // initial encoding data Two-dimensional array
    ctx->uctx->parity_logging_data = (u8 *)((uint64_t)ctx->kv_info->l_addr + 
        ctx->kv_all_len + sizeof(ParityLogging));
    // xor operation
    for(int i = 0;i < ctx->kv_all_len;i ++){
        ctx->uctx->parity_logging_data[i] = D2[i] ^ D1[i];
    }
    void *send_mes = (void *)((uint64_t)ctx->kv_info->l_addr + ctx->kv_all_len);
    ParityLogging *parity_logging = (ParityLogging *)send_mes;
    parity_logging->len = ctx->kv_all_len;
    parity_logging->off = ctx->uctx->kv_meta_ctx.off;
    parity_logging->crc = 0;
    if(if_req_latency){
        ctx->req_timer->end_timer(UPDATE_ENCODING_PARITY);
    }
}

void Client::kv_update_get_PL_hash_info(UpdateCtx * uctx){
    int stripe_id = uctx->stripe_id;
    uint64_t pre_key;
    for(int i = 0;i < m;i ++){
        uctx->PL_info[i].lkey = input_buf_mr_->lkey;
        pre_key = connect_parity_key_to_64int(stripe_id, uctx->parity_id[i]);
        uctx->PL_hash_info[i].hash_value = hash_64int_to_64int(pre_key);
        uctx->PL_hash_info[i].fp = hash_index_compute_fp(uctx->PL_hash_info[i].hash_value);
    }
}

void Client::kv_update_get_PL_addr_info(UpdateCtx * uctx){
    get_two_combined_bucket_index(&uctx->PL_hash_info[0], uctx->PL_addr_info);
    get_PL_server_subtable_addr(&uctx->PL_hash_info[0], uctx->PL_addr_info, uctx->parity_id);
}

void Client::kv_update_prepare_PL(KVReqCtx * ctx){
    kv_update_prepare_PL_info(ctx);
    kv_update_get_PL_hash_info(ctx->uctx);
    kv_update_get_PL_addr_info(ctx->uctx);
}

void Client::kv_update_prepare_read_PL_bucket_sr_list(KVReqCtx * ctx){
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_READ_PL_BUCKET);
    }
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    uint8_t server_id;
    server_id = ctx->uctx->PL_addr_info->server_id_list[0];
    sr_list = gen_bucket_sr_lists(ctx->coro_id, ctx->uctx->PL_addr_info,
        &ctx->uctx->PL_bucket_buffer[0], &sr_list_num, READ_PARITY_LOGGING_BUCKET);
    if(if_req_latency){
        ctx->wr_id_map_op_id[sr_list->sr_list[1].wr_id] = UPDATE_READ_PL_BUCKET;
    }
    kv_update_send(ctx->uctx, sr_list, sr_list_num);
}

void Client::kv_update_prepare_write_PL_sr_list(KVReqCtx * ctx){
    UpdateCtx *uctx = ctx->uctx;
    uint64_t hash_value;
    int server_id;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    for(int i = 0;i < m;i ++){
        if(if_req_latency){
            if(i == 0){
                ctx->req_timer->start_timer(UPDATE_WRITE_PL1);
                ctx->req_timer->start_timer(UPDATE_WRITE_PL_LOG1);
            } else if(i == 1){
                ctx->req_timer->start_timer(UPDATE_WRITE_PL2);
                ctx->req_timer->start_timer(UPDATE_WRITE_PL_LOG2);
            }
        }
        server_id = ctx->uctx->PL_addr_info->server_id_list[i];
        mm_->mm_alloc(uctx->parity_logging_size, nm_, 
            server_id, &ctx->uctx->mm_alloc_parity_logging_ctx[i]);
        sr_list = gen_write_kv_sr_lists(ctx->coro_id, &ctx->uctx->PL_info[i], 
            &ctx->uctx->mm_alloc_parity_logging_ctx[i], &sr_list_num,
            WRITE_PARITY_LOGGING);
        if(if_req_latency){
            if(i == 0){
                ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_WRITE_PL1;
            } else if(i == 1){
                ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_WRITE_PL2;
            }
        }
        kv_update_send(uctx, sr_list, sr_list_num);
        // add log batch
        // make_log_entry((LogEntry *)uctx->PL_slot_log_buf_addr[i], LOG_UP_DE_CAS_PL_SLOT,
        //     uctx->mm_alloc_parity_logging_ctx[i].addr_list);
        // sr_list = gen_sr_list(uctx->PL_slot_log_buf_addr[i], LOG_ENTRY_LEN, 
        //     local_buf_mr_->lkey, uctx->coro_id, server_id, UPDATE_WRITE_PL_SLOT_LOG, i,
        //     IBV_WR_RDMA_WRITE, get_log_entry_addr(server_id));
        // if(if_req_latency){
        //     if(i == 0){
        //         ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_WRITE_PL_LOG1;
        //     } else if(i == 1){
        //         ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_WRITE_PL_LOG2;
        //     }
        // }
        // kv_update_send(uctx, sr_list, 1);
    }
}

void Client::kv_update_write_kv_log(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    uint8_t server_id = ctx->tbl_addr_info.server_id_list[0];
    make_log_entry((LogEntry *)uctx->kv_slot_log_buf_addr, LOG_UPDATE_CAS_KV_SLOT,
        ctx->mm_alloc_ctx.addr_list);
    IbvSrList *sr_list;
    sr_list = gen_sr_list(uctx->kv_slot_log_buf_addr, LOG_ENTRY_LEN, local_buf_mr_->lkey,
        uctx->coro_id, server_id, UPDATE_WRITE_KV_SLOT_LOG, 0, IBV_WR_RDMA_WRITE, 
        get_log_entry_addr(server_id));
    if(if_req_latency){
        ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_WRITE_KV_LOG;
    }
    kv_update_send(uctx, sr_list, 1);
}

void Client::kv_update_RTT3_check(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    KVCASAddr cas_addr = uctx->stripe_meta_bucket_info.kv_modify_pr_cas_list[0];
    IbvSrList *sr_list;
    uint32_t sr_list_num;
    int check_stripe_meta;
    if(uctx->is_cas == true){
        // cas failed choose faa
        if(cas_addr.orig_value != *(uint64_t *)cas_addr.l_kv_addr){
            check_stripe_meta = check_stripe_meta_wl((StripeMetaSlot *)cas_addr.l_kv_addr, 
                &uctx->stripe_meta_ctx);
            if(check_stripe_meta == -1){
                // RDMA_LOG_IF(2, if_print_log) << "client id:" << client_id << " cas failed~ " 
                //     << "old WL:" << uctx->stripe_meta_ctx.WL
                //     << "new WL:" << check_stripe_meta;
                ctx->is_finished = true;
                ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
                code_num_count.update_RTT3_cas_return_invalied ++;
                return;
            } else {
                // print_error("faa recovery~");
                kv_update_init_sr_list(ctx);
                sr_list = gen_faa_sr_lists(uctx->coro_id,
                    uctx->stripe_meta_bucket_info.kv_modify_pr_cas_list,
                    &sr_list_num, faa_need_value_WL);

                send_one_sr_list(sr_list, uctx->comp_wrid_map);
                poll_completion(*uctx->comp_wrid_map);
            }
        }
    }

    if(uctx->is_cas == false){
        check_stripe_meta = check_stripe_meta_wl((StripeMetaSlot *)cas_addr.l_kv_addr, 
            &uctx->stripe_meta_ctx);
        StripeMetaSlot * faa_stripe_meta = (StripeMetaSlot *)cas_addr.l_kv_addr;
        StripeMetaCtx stripe_meta_ctx;
        get_stripe_meta_slot(faa_stripe_meta, &stripe_meta_ctx);
        if(check_stripe_meta == -1){
            kv_update_init_sr_list(ctx);
            sr_list = gen_faa_sr_lists(uctx->coro_id,
                uctx->stripe_meta_bucket_info.kv_modify_pr_cas_list,
                &sr_list_num, -faa_need_value_WL);
            send_one_sr_list(sr_list, uctx->comp_wrid_map);
            poll_completion(*uctx->comp_wrid_map);
            ctx->is_finished = true;
            ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
            code_num_count.update_RTT3_faa_return_invalied ++;
            return;
        }
    }
}

void Client::kv_degread_RTT3_check(KVReqCtx *ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    KVCASAddr cas_addr = drctx->stripe_meta_bucket_buffer->kv_modify_pr_cas_list[0];
    IbvSrList *sr_list;
    uint32_t sr_list_num;
    int check_stripe_meta;
    if(drctx->is_cas == true){
        // cas failed choose faa
        if(cas_addr.orig_value != *(uint64_t *)cas_addr.l_kv_addr){
            check_stripe_meta = check_stripe_meta_rl((StripeMetaSlot *)cas_addr.l_kv_addr, 
                drctx->stripe_meta_ctx);
            if(check_stripe_meta == -1){
                ctx->is_finished = true;
                ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
                code_num_count.update_RTT3_cas_return_invalied ++;
                return;
            } else {
                // print_error("faa recovery~");
                kv_degread_init_sr_list(ctx);
                sr_list = gen_faa_sr_lists(drctx->coro_id,
                    drctx->stripe_meta_bucket_buffer->kv_modify_pr_cas_list,
                    &sr_list_num, faa_need_value_RL);
                send_one_sr_list(sr_list, drctx->comp_wrid_map);
                poll_completion(*drctx->comp_wrid_map);
            }
        }
    }

    if(drctx->is_cas == false){
        check_stripe_meta = check_stripe_meta_rl((StripeMetaSlot *)cas_addr.l_kv_addr, 
            drctx->stripe_meta_ctx);
        StripeMetaSlot * faa_stripe_meta = (StripeMetaSlot *)cas_addr.l_kv_addr;
        StripeMetaCtx stripe_meta_ctx;
        get_stripe_meta_slot(faa_stripe_meta, &stripe_meta_ctx);
        if(check_stripe_meta == -1){
            kv_degread_init_sr_list(ctx);
            sr_list = gen_faa_sr_lists(drctx->coro_id,
                drctx->stripe_meta_bucket_buffer->kv_modify_pr_cas_list,
                &sr_list_num, -faa_need_value_RL);
            send_one_sr_list(sr_list, drctx->comp_wrid_map);
            poll_completion(*drctx->comp_wrid_map);
            ctx->is_finished = true;
            ctx->ret_val.value_addr = NULL;
            code_num_count.update_RTT3_faa_return_invalied ++;
            return;
        }
    }
}

void Client::kv_update_RTT3_sync(KVReqCtx * ctx){
    kv_update_init_sr_list(ctx);
    // check kv is true
    ctx->uctx->PL_match_idx = find_match_kv_idx(ctx);
    if (ctx->uctx->PL_match_idx == -1) {
        // print_error("update workflow:find kv failed in prepare PL info~");
        code_num_count.cache_miss ++;
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    kv_update_cas_stripe_meta_first(ctx);
    if(ctx->uctx->is_finished == true){
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    // get parity logging bucket and write parity logging
    kv_update_prepare_PL(ctx);
    // kv_update_write_kv_log(ctx);
    kv_update_prepare_write_PL_sr_list(ctx);
    kv_update_prepare_read_PL_bucket_sr_list(ctx);
    nm_->rdma_post_check_sr_list(ctx->uctx->comp_wrid_map);
    kv_update_RTT3_check(ctx);
}

void Client::kv_update_RTT3_async(KVReqCtx * ctx){
    kv_update_init_sr_list(ctx);
    // check kv is true
    ctx->uctx->PL_match_idx = find_match_kv_idx(ctx);
    if (ctx->uctx->PL_match_idx == -1) {
        // print_error("update workflow:find kv failed in prepare PL info~");
        code_num_count.update_match_kv_failed ++;
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    kv_update_cas_stripe_meta_first(ctx);
    if(ctx->uctx->is_finished == true){
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    // get parity logging bucket and write parity logging
    kv_update_prepare_PL(ctx);
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_WRITE_KV_LOG);
    }
    // kv_update_write_kv_log(ctx);
    kv_update_prepare_write_PL_sr_list(ctx);
    kv_update_prepare_read_PL_bucket_sr_list(ctx);
    kv_update_post_and_wait(ctx);
    kv_update_RTT3_check(ctx);
}

void Client::kv_update_RTT5_log(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_WRITE_END_LOG);
    }
    // write stripe id to hit the update is finished
    make_log_entry((LogEntry *)uctx->stripe_meta_end_log_buf_addr,
        LOG_UPDATE_END, uctx->stripe_id);
    IbvSrList * sr_list;
    uint8_t server_id = uctx->stripe_meta_addr_info->server_id_list[0];
    sr_list = gen_sr_list(uctx->stripe_meta_end_log_buf_addr,
        LOG_ENTRY_LEN, local_buf_mr_->lkey, uctx->coro_id, server_id, UPDATE_LOG_END,
        0, IBV_WR_RDMA_WRITE, get_log_entry_addr(server_id));
    if(if_req_latency){
        ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_WRITE_END_LOG;
    }
    kv_update_send(uctx, sr_list, 1);
}

void Client::kv_update_RTT5_sync(KVReqCtx * ctx){
    kv_update_init_sr_list(ctx);
    kv_update_cas_stripe_meta_second(ctx);
    // kv_update_RTT5_log(ctx->uctx);
    nm_->rdma_post_check_sr_list(ctx->uctx->comp_wrid_map);
}

void Client::kv_update_RTT5_async(KVReqCtx * ctx){
    kv_update_init_sr_list(ctx);
    kv_update_cas_stripe_meta_second(ctx);
    // kv_update_RTT5_log(ctx);
    kv_update_post_and_wait(ctx);
}

void Client::kv_update_cas_stripe_meta_second(KVReqCtx *ctx){
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_FAA_STRIPE_META);
    }
    kv_update_cas_stripe_meta(ctx, false);
}

int Client::kv_update_sync(KVReqCtx * ctx) {
    kv_update_RTT1_sync(ctx);
    kv_update_RTT2_sync(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.ret_code;
    }
    kv_update_RTT3_sync(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.ret_code;
    }
    kv_update_RTT4_sync(ctx);
    kv_update_RTT5_sync(ctx);
    return ctx->ret_val.ret_code;
}

void Client::kv_update_cas_rec_RTT1(KVReqCtx *ctx, CasRecoveryEle *cas_rec_ele){
    ctx->comp_wrid_map.clear();
    CasKvMessage *cas_mes = &cas_rec_ele->cas_kv_mes;
    IbvSrList *sr_list = gen_sr_list(cas_mes->local_addr, 
        sizeof(uint64_t), local_buf_mr_->lkey, ctx->coro_id, cas_mes->server_id, 
        UPDATE_RECOVERY_READ_SLOT, 0, IBV_WR_RDMA_READ, cas_mes->remote_slot_addr);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    poll_completion(ctx->comp_wrid_map);
}

void Client::kv_update_cas_rec_RTT2_check(KVReqCtx *ctx, CasRecoveryEle *cas_rec_ele, uint64_t old_value){
    CasKvMessage *cas_mes = &cas_rec_ele->cas_kv_mes;
    uint64_t local_addr = cas_mes->local_addr;
    if(*(uint64_t *)local_addr == old_value){
        code_num_count.update_recovery_kv_num ++;
        ctx->ret_val.ret_code = KV_OPS_SUCCESS; 
    } else {
        // print_error("update workflow:RTT4 cas kv slot failed~");
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        cas_rec_ele->recovery_times --;
        cas_rec_ele->recovery_when = rand() % RAND_RECOVERY_TIMES;
        int coro_id = ctx->coro_id;
        if(cas_rec_ele->recovery_times != 0){
            cas_rec->insert(coro_id, cas_rec_ele);
        }
    }
    ctx->is_finished = true;
    cas_rec->remove(ctx->coro_id);
}

IbvSrList *Client::gen_one_cas_sr_list(uint64_t local_addr, uint32_t lkey, uint32_t coro_id, uint8_t server_id,
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

void Client::kv_update_cas_rec_RTT2(KVReqCtx *ctx, CasRecoveryEle *cas_rec_ele){
    ctx->comp_wrid_map.clear();
    CasKvMessage *cas_mes = &cas_rec_ele->cas_kv_mes;
    IbvSrList *sr_list;
    uint64_t old_value = *(uint64_t *)cas_mes->local_addr;
    sr_list = gen_one_cas_sr_list(cas_mes->local_addr, local_buf_mr_->lkey, ctx->coro_id, 
        cas_mes->server_id, CAS_ST_WRID, 0, cas_mes->remote_slot_addr, old_value, 
        cas_mes->new_value, IBV_SEND_SIGNALED, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    poll_completion(ctx->comp_wrid_map);
    kv_update_cas_rec_RTT2_check(ctx, cas_rec_ele, old_value);
}

int Client::kv_update_cas_rec(KVReqCtx *ctx, CasRecoveryEle *cas_rec_ele){
    ctx->is_finished = false;
    ctx->ret_val.ret_code = 0;
    kv_update_cas_rec_RTT1(ctx, cas_rec_ele);
    kv_update_cas_rec_RTT2(ctx, cas_rec_ele);
    return ctx->ret_val.ret_code;
}

int Client::kv_update(KVReqCtx * ctx) {
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_ALL);
    }
    kv_update_RTT1_async(ctx);
    kv_update_RTT2_async(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.ret_code;
    }
    kv_update_RTT3_async(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.ret_code;
    }
    kv_update_RTT4_async(ctx);
    kv_update_RTT5_async(ctx);
    if(if_req_latency){
        ctx->req_timer->end_timer(UPDATE_ALL);
        if(ctx->ret_val.ret_code != KV_OPS_FAIL_RETURN){
        client_add_req_latency(ctx->req_timer);
    }
    }
    return ctx->ret_val.ret_code;
}   

void Client::kv_delete_init_sr_list(DeleteCtx *delete_ctx){
    delete_ctx->comp_wrid_map->clear();
}

void Client::kv_delete_analysis_stripe_meta(DeleteCtx *dctx){
    dctx->is_finished = false;
    get_bucket_info(dctx->stripe_meta_bucket_info);
    bool if_find = false;
    if_find = find_stripe_meta_slot_struct(dctx->stripe_meta_bucket_info,
        dctx->stripe_meta_ctx, dctx->stripe_id,
        dctx->stripe_meta_idx);
    if(if_find == false){
        print_error("delete workflow:analysis stripe meta bucket:find slot failed~");
        dctx->is_finished = true;
        dctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        code_num_count.update_RTT2_read_stripe_meta_failed ++;
        return;
    }
    if(dctx->stripe_meta_ctx->WL == pow(2, 5) - 1){
        print_mes("delete workflow:analysis stripe meta bucket:find WL is full!");
        dctx->is_finished = true;
        dctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        code_num_count.update_RTT2_read_wl_is_15 ++;
        return;
    }
    if(dctx->stripe_meta_ctx->WL == 0){
        dctx->is_cas = true;
    } else {
        dctx->is_cas = false;
    }
    if(dctx->stripe_meta_ctx->RL != 0){
        RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " <<
        "delete workflow:analysis stripe meta bucket:find RL is not 0(there is degrade read doing)!";
        exit(0);
    }
    for(int i = 0;i < m - 1;i ++){
        dctx->parity_id[i + 1] = dctx->stripe_meta_ctx->pid[i];
    }
}

void Client::kv_delete_make_cas_stripe_meta(DeleteCtx *dctx){
    int bucket_idx = dctx->stripe_meta_idx->bucket_idx;
    int slot_idx = dctx->stripe_meta_idx->slot_idx;
    uint64_t local_com_bucket_addr, old_local_slot_addr, remote_slot_addr;
    if(bucket_idx < 2){
        local_com_bucket_addr = (uint64_t)dctx->stripe_meta_bucket_info->f_com_bucket;
        old_local_slot_addr = (uint64_t)&dctx->
            stripe_meta_bucket_info->f_com_bucket[bucket_idx].slots[slot_idx];
        remote_slot_addr = dctx->stripe_meta_addr_info->f_bucket_addr[0] + 
            (old_local_slot_addr - local_com_bucket_addr);
    } else {
        local_com_bucket_addr = (uint64_t)dctx->stripe_meta_bucket_info->s_com_bucket;
        old_local_slot_addr = (uint64_t)&dctx->
            stripe_meta_bucket_info->s_com_bucket[bucket_idx - 2].slots[slot_idx];
        remote_slot_addr = dctx->stripe_meta_addr_info->s_bucket_addr[0] + 
            (old_local_slot_addr - local_com_bucket_addr);
    }
    uint64_t old_value = *(uint64_t *)old_local_slot_addr;
    StripeMetaSlot *stripe_meta = (StripeMetaSlot *)old_local_slot_addr;
    write_stripe_meta_wl(stripe_meta, 1);
    uint64_t new_value = *(uint64_t *)old_local_slot_addr;
    fill_cas_slot_through_bucket_buffer_value(
        &dctx->stripe_meta_bucket_info->kv_modify_pr_cas_list[0],
        dctx->stripe_meta_addr_info->server_id_list[0],
        local_buf_mr_->lkey, remote_slot_addr,
        (uint64_t)dctx->stripe_meta_bucket_info->local_cas_return_value_addr,
        old_value, new_value
    );
}

void Client::kv_delete_cas_stripe_meta(DeleteCtx *dctx, bool if_add){
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    if(if_add == false){
        sr_list = gen_faa_sr_lists(dctx->coro_id, 
            dctx->stripe_meta_bucket_info->kv_modify_pr_cas_list,
            &sr_list_num, -faa_need_value_WL);
        kv_delete_send(dctx, sr_list, sr_list_num);
        return;
    }
    kv_delete_make_cas_stripe_meta(dctx);
    if(dctx->is_cas == true){
        sr_list = gen_cas_sr_lists(dctx->coro_id,
            dctx->stripe_meta_bucket_info->kv_modify_pr_cas_list,
            &sr_list_num);
    } else {
        sr_list = gen_faa_sr_lists(dctx->coro_id,
            dctx->stripe_meta_bucket_info->kv_modify_pr_cas_list,
            &sr_list_num, faa_need_value_WL);
    }
    kv_delete_send(dctx, sr_list, sr_list_num);
}

void Client::kv_delete_write_stripe_meta_log(DeleteCtx *dctx){
    uint8_t log_type;
    uint32_t wr_id_type;
    if(dctx->is_cas == true){
        log_type = LOG_UP_DE_CAS_STRIPE_META;
        wr_id_type = DELETE_CAS_STRIPE_META_LOG;
    } else {
        log_type = LOG_UP_DE_FAA_STRIPE_META;
        wr_id_type = DELETE_FAA_STRIPE_META_LOG;
    }
    uint8_t server_id = dctx->stripe_meta_addr_info->server_id_list[0];
    make_log_entry((LogEntry *)dctx->stripe_meta_log_buf_addr, log_type, 
        dctx->stripe_meta_idx->slot_value);
    IbvSrList * sr_list;
    sr_list = gen_sr_list(dctx->stripe_meta_log_buf_addr,
        LOG_ENTRY_LEN, local_buf_mr_->lkey, dctx->coro_id, server_id, wr_id_type,
        0, IBV_WR_RDMA_WRITE, get_log_entry_addr(server_id));
    send_one_sr_list(sr_list, dctx->comp_wrid_map);
}

void Client::kv_delete_cas_stripe_meta_first(DeleteCtx *dctx){
    kv_delete_analysis_stripe_meta(dctx);
    if(dctx->is_finished == true){
        return;
    }
    // kv_delete_write_stripe_meta_log(dctx);
    kv_delete_cas_stripe_meta(dctx, true);
}

void Client::kv_delete_get_PL_hash_info(DeleteCtx *dctx){
    int stripe_id = dctx->stripe_id;
    uint64_t pre_key;
    for(int i = 0;i < m;i ++){
        dctx->PL_info[i].lkey = input_buf_mr_->lkey;
        pre_key = connect_parity_key_to_64int(stripe_id, dctx->parity_id[i]);
        dctx->PL_hash_info[i].hash_value = hash_64int_to_64int(pre_key);
        dctx->PL_hash_info[i].fp = hash_index_compute_fp(dctx->PL_hash_info[i].hash_value);
    }
}

void Client::kv_delete_get_PL_addr_info(DeleteCtx *dctx){
    get_two_combined_bucket_index(dctx->PL_hash_info, dctx->PL_addr_info);
    get_PL_server_subtable_addr(dctx->PL_hash_info, dctx->PL_addr_info, dctx->parity_id);
}

void Client::kv_delete_prepare_PL(KVReqCtx *ctx){
    kv_delete_compute_PL(ctx);
    kv_delete_get_PL_hash_info(ctx->dctx);
    kv_delete_get_PL_addr_info(ctx->dctx);
}

void Client::kv_delete_compute_PL(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    u8 *D1 = (u8 *)ctx->kv_read_addr_list[ctx->dctx->PL_match_idx].l_kv_addr;
    for(int i = 0;i < ctx->kv_all_len;i ++){
        dctx->parity_logging_data[i] = D1[i];
    }
    void *send_mes = (u8 *)((uint64_t)ctx->kv_info->l_addr + ctx->kv_all_len);
    ParityLogging *parity_logging = (ParityLogging *)send_mes;
    parity_logging->len = ctx->kv_all_len;
    parity_logging->off = dctx->kv_meta_ctx->off;
    parity_logging->crc = 0;
}

bool Client::find_empty_slot_from_bucket_buffer(BucketBuffer *bucketbuffer, 
    TagKVTableAddrInfo *addr_info){
    uint32_t f_free_num, s_free_num, f_main_idx, s_main_idx, f_free_slot_idx, s_free_slot_idx;
    int32_t bucket_idx = -1, slot_idx = -1;
    int num_filled_combined_bucket = 0;
    f_main_idx = addr_info->f_main_idx;
    s_main_idx = addr_info->s_main_idx;
    for (int j = 0; j < 2; j ++) {
        f_free_num = get_free_slot_num(bucketbuffer->f_com_bucket + f_main_idx, &f_free_slot_idx);
        s_free_num = get_free_slot_num(bucketbuffer->s_com_bucket + s_main_idx, &s_free_slot_idx);
        if(f_free_num ==0 && s_free_num == 0){
            num_filled_combined_bucket ++;
            if(num_filled_combined_bucket == 2){
                bucketbuffer->bucket_idx = 0;
                bucketbuffer->slot_idx   = 0;   
                return false;
            }
        }
        if (f_free_num > 0 || s_free_num > 0) {
            if (f_free_num >= s_free_num) {
                bucket_idx = f_main_idx;
                slot_idx = f_free_slot_idx;
            } else {
                bucket_idx = 2 + s_main_idx;
                slot_idx = s_free_slot_idx;
            }
        }
        f_main_idx = (f_main_idx + 1) % 2;
        s_main_idx = (s_main_idx + 1) % 2;
    }
    bucketbuffer->bucket_idx = bucket_idx;
    bucketbuffer->slot_idx   = slot_idx;   
    return true; 
}

bool Client::find_empty_slot_from_bucket_buffer_big(BucketBuffer *bucketbuffer, 
    TagKVTableAddrInfo *addr_info){
    uint32_t f_free_num, s_free_num, f_main_idx, s_main_idx, f_free_slot_idx, s_free_slot_idx;
    int32_t bucket_idx = -1, slot_idx = -1;
    int num_filled_combined_bucket = 0;
    f_main_idx = addr_info->f_main_idx;
    s_main_idx = addr_info->s_main_idx;
    for (int j = 0; j < 2; j ++) {
        f_free_num = get_free_kv_ec_meta_slot_num((KvEcMetaBucket *)bucketbuffer->f_com_bucket + f_main_idx * 2, &f_free_slot_idx);
        s_free_num = get_free_kv_ec_meta_slot_num((KvEcMetaBucket *)bucketbuffer->s_com_bucket + s_main_idx * 2, &s_free_slot_idx);
        if(f_free_num ==0 && s_free_num == 0){
            num_filled_combined_bucket ++;
            if(num_filled_combined_bucket == 2){
                bucketbuffer->bucket_idx = 0;
                bucketbuffer->slot_idx = 0;
                return false;
            }
        }
        if (f_free_num > 0 || s_free_num > 0) {
            if (f_free_num >= s_free_num) {
                bucket_idx = f_main_idx;
                slot_idx = f_free_slot_idx;
            } else {
                bucket_idx = 2 + s_main_idx;
                slot_idx = s_free_slot_idx;
            }
        }
        f_main_idx = (f_main_idx + 1) % 2;
        s_main_idx = (s_main_idx + 1) % 2;
    }
    bucketbuffer->bucket_idx = bucket_idx;
    bucketbuffer->slot_idx   = slot_idx;   
    return true;  
}

void Client::kv_delete_cas_stripe_meta_second(DeleteCtx *dlctx){
    kv_delete_cas_stripe_meta(dlctx, false);
}

int Client::kv_delete_sync(KVReqCtx * ctx) {
    kv_delete_RTT1_sync(ctx);
    kv_delete_RTT2_sync(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.ret_code;
    }
    kv_delete_RTT3_sync(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.ret_code;
    }
    kv_delete_RTT4_sync(ctx);
    kv_delete_RTT5_sync(ctx);
    return ctx->ret_val.ret_code;
}

void Client::kv_delete_send(DeleteCtx *dctx, IbvSrList *sr_list, uint32_t sr_list_num){
    send_one_sr_list(sr_list, dctx->comp_wrid_map);
}

IbvSrList *Client::gen_one_sr_list(uint64_t local_addr, uint32_t local_len, uint32_t lkey, 
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

void Client::kv_delete_RTT1_read_bucket(KVReqCtx *ctx){
    // prepare write kv and read bucket sr lists
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    uint8_t server_id = ctx->tbl_addr_info.server_id_list[0];
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, server_id, READ_KV_BUCKET, 1, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    kv_delete_send(ctx->dctx, sr_list, 1);
}

void Client::kv_delete_post_and_wait(DeleteCtx *dctx){
    poll_completion(*dctx->comp_wrid_map);
}

void Client::kv_delete_RTT1_sync(KVReqCtx *ctx){
    ctx->is_finished = false;
    kv_delete_init_sr_list(ctx->dctx);
    kv_delete_RTT1_read_bucket(ctx);
    nm_->rdma_post_check_sr_list(ctx->dctx->comp_wrid_map);
}

void Client::kv_delete_RTT1_async(KVReqCtx *ctx){
    ctx->is_finished = false;
    kv_delete_init_sr_list(ctx->dctx);
    kv_delete_RTT1_read_bucket(ctx);
    kv_delete_post_and_wait(ctx->dctx);
}

void Client::delete_analysis_kv_meta(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    KvEcMetaBucket *bucket = (KvEcMetaBucket *)ctx->local_bucket_addr;
    EcMetaSlot *slot = &bucket[ctx->bucket_idx].slots[ctx->slot_idx].ec_meta_slot;
    dctx->old_kv_meta_value = *(uint64_t *)slot;
    get_ec_meta_slot(slot, dctx->kv_meta_ctx);
    dctx->parity_id[0] = dctx->kv_meta_ctx->pid1;
    dctx->stripe_id = dctx->kv_meta_ctx->sid;
}

void Client::kv_delete_prepare_stripe_meta(DeleteCtx *dctx){
    prepare_stripe_meta(dctx->stripe_id, dctx->stripe_meta_hash_info, dctx->stripe_meta_addr_info);
}

void Client::kv_delete_get_stripe_meta(KVReqCtx *ctx){
    delete_analysis_kv_meta(ctx);
    if(ctx->is_finished == true){
        return;
    }
    kv_delete_prepare_stripe_meta(ctx->dctx);
    uint32_t sr_list_num;
    IbvSrList * sr_list = gen_bucket_sr_lists(ctx->coro_id, 
        ctx->dctx->stripe_meta_addr_info, 
        ctx->dctx->stripe_meta_bucket_info, 
        &sr_list_num, READ_STRIPE_META_BUCKET);
    kv_delete_send(ctx->dctx, sr_list, sr_list_num);
}

void Client::kv_delete_RTT2_sync(KVReqCtx *ctx){
    kv_delete_init_sr_list(ctx->dctx);
    find_kv_slot(ctx);
    if(ctx->is_finished == true){
        return;
    }
    kv_delete_get_stripe_meta(ctx);
    nm_->rdma_post_check_sr_list(ctx->dctx->comp_wrid_map);
}

void Client::kv_delete_RTT2_async(KVReqCtx *ctx){
    kv_delete_init_sr_list(ctx->dctx);
    find_kv_slot(ctx);
    if(ctx->is_finished == true){
        // ctx->ret_val.ret_code = KV_OPS_SUCCESS;
        return;
    }
    kv_delete_get_stripe_meta(ctx);
    kv_delete_post_and_wait(ctx->dctx);
}

void Client::kv_delete_write_kv_log(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    uint8_t server_id = ctx->tbl_addr_info.server_id_list[0];
    make_log_entry((LogEntry *)dctx->kv_slot_log_buf_addr, LOG_DELETE_OLD_KV_SLOT,
        ctx->kv_read_addr_list[0].r_kv_addr);
    IbvSrList *sr_list;
    sr_list = gen_sr_list(dctx->kv_slot_log_buf_addr, LOG_ENTRY_LEN, local_buf_mr_->lkey,
        dctx->coro_id, server_id, DELETE_WRITE_KV_SLOT_LOG, 0, IBV_WR_RDMA_WRITE, 
        get_log_entry_addr(server_id));
    kv_delete_send(dctx, sr_list, 1);
}

void Client::kv_delete_prepare_write_PL_sr_list(KVReqCtx * ctx){
    DeleteCtx *dctx = ctx->dctx;
    uint64_t hash_value;
    int server_id;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    for(int i = 0;i < m;i ++){
        server_id = dctx->PL_addr_info->server_id_list[i];
        mm_->mm_alloc(dctx->parity_logging_size, nm_, 
            server_id, &dctx->mm_alloc_parity_logging_ctx[i]);
        sr_list = gen_write_kv_sr_lists(ctx->coro_id, &dctx->PL_info[i], 
            &dctx->mm_alloc_parity_logging_ctx[i], &sr_list_num,
            WRITE_PARITY_LOGGING);
        kv_delete_send(dctx, sr_list, sr_list_num);
        // make_log_entry((LogEntry *)dctx->PL_slot_log_buf_addr[i], LOG_UP_DE_CAS_PL_SLOT,
        //     dctx->mm_alloc_parity_logging_ctx[i].addr_list);
        // sr_list = gen_sr_list(dctx->PL_slot_log_buf_addr[i], LOG_ENTRY_LEN, 
        //     local_buf_mr_->lkey, dctx->coro_id, server_id, DELETE_WRITE_PL_SLOT_LOG, i,
        //     IBV_WR_RDMA_WRITE, get_log_entry_addr(server_id));
        // kv_delete_send(dctx, sr_list, 1);
    }
}

void Client::kv_delete_prepare_read_PL_bucket_sr_list(KVReqCtx * ctx){
    DeleteCtx *dctx = ctx->dctx;
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    uint8_t server_id;
    server_id = dctx->PL_addr_info->server_id_list[0];
    sr_list = gen_bucket_sr_lists(ctx->coro_id, dctx->PL_addr_info,
        dctx->PL_bucket_buffer, &sr_list_num, READ_PARITY_LOGGING_BUCKET);
    kv_delete_send(ctx->dctx, sr_list, sr_list_num);
}

void Client::kv_delete_RTT3_write_kv_meta_log(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    uint8_t server_id = ctx->tbl_addr_info.server_id_list[0];
    make_log_entry((LogEntry *)dctx->kv_slot_log_buf_addr, LOG_DELETE_OLD_KV_META,
        dctx->old_kv_meta_value);
    IbvSrList *sr_list;
    sr_list = gen_sr_list(dctx->kv_slot_log_buf_addr, LOG_ENTRY_LEN, local_buf_mr_->lkey,
        dctx->coro_id, server_id, DELETE_WRITE_KV_META_LOG, 0, IBV_WR_RDMA_WRITE, 
        get_log_entry_addr(server_id));
    kv_delete_send(dctx, sr_list, 1);
}

void Client::kv_delete_RTT3_check(KVReqCtx *ctx){
    DeleteCtx *dctx = ctx->dctx;
    KVCASAddr cas_addr = dctx->stripe_meta_bucket_info->kv_modify_pr_cas_list[0];
    IbvSrList *sr_list;
    uint32_t sr_list_num;
    int check_stripe_meta;
    if(dctx->is_cas == true){
        // cas failed choose faa
        if(cas_addr.orig_value != *(uint64_t *)cas_addr.l_kv_addr){
            // print_error("cas failed~");
            // code_num_count.RTT3_cas_return_invalied ++;
            check_stripe_meta = check_stripe_meta_wl((StripeMetaSlot *)cas_addr.l_kv_addr, 
                dctx->stripe_meta_ctx);
            if(check_stripe_meta == -1){
                RDMA_LOG_IF(2, if_print_log) << "client id:" << client_id << " cas failed~ " 
                    << "old WL:" << dctx->stripe_meta_ctx->WL
                    << "new WL:" << check_stripe_meta;
                ctx->is_finished = true;
                ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
                code_num_count.update_RTT3_cas_return_invalied ++;
                return;
            } else {
                // print_error("faa recovery~");
                kv_delete_init_sr_list(ctx->dctx);
                sr_list = gen_faa_sr_lists(dctx->coro_id,
                    dctx->stripe_meta_bucket_info->kv_modify_pr_cas_list,
                    &sr_list_num, faa_need_value_WL);
                send_one_sr_list(sr_list, dctx->comp_wrid_map);
                poll_completion(*dctx->comp_wrid_map);
            }
        }
    }

    if(dctx->is_cas == false){
        check_stripe_meta = check_stripe_meta_wl((StripeMetaSlot *)cas_addr.l_kv_addr, 
            dctx->stripe_meta_ctx);
        StripeMetaSlot * faa_stripe_meta = (StripeMetaSlot *)cas_addr.l_kv_addr;
        StripeMetaCtx stripe_meta_ctx;
        get_stripe_meta_slot(faa_stripe_meta, &stripe_meta_ctx);
        if(check_stripe_meta == -1){
            print_error("faa failed~");
            kv_delete_init_sr_list(ctx->dctx);
            sr_list = gen_faa_sr_lists(dctx->coro_id,
                dctx->stripe_meta_bucket_info->kv_modify_pr_cas_list,
                &sr_list_num, -faa_need_value_WL);
            send_one_sr_list(sr_list, dctx->comp_wrid_map);
            poll_completion(*dctx->comp_wrid_map);
            ctx->is_finished = true;
            ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
            code_num_count.update_RTT3_faa_return_invalied ++;
            return;
        }
    }
}

void Client::kv_delete_RTT3_sync(KVReqCtx * ctx){
    kv_delete_init_sr_list(ctx->dctx);
    kv_delete_cas_stripe_meta_first(ctx->dctx);
    if(ctx->dctx->is_finished == true){
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    kv_delete_prepare_PL(ctx);
    // kv_delete_write_kv_log(ctx);
    kv_delete_prepare_write_PL_sr_list(ctx);
    kv_delete_prepare_read_PL_bucket_sr_list(ctx);
    kv_delete_RTT3_write_kv_meta_log(ctx);
    nm_->rdma_post_check_sr_list(ctx->dctx->comp_wrid_map);
    kv_delete_RTT3_check(ctx);
}

void Client::kv_delete_RTT3_async(KVReqCtx * ctx){
    kv_delete_init_sr_list(ctx->dctx);
    kv_delete_cas_stripe_meta_first(ctx->dctx);
    if(ctx->dctx->is_finished == true){
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    kv_delete_prepare_PL(ctx);
    kv_delete_write_kv_log(ctx);
    kv_delete_prepare_write_PL_sr_list(ctx);
    kv_delete_prepare_read_PL_bucket_sr_list(ctx);
    kv_delete_RTT3_write_kv_meta_log(ctx);
    kv_delete_post_and_wait(ctx->dctx);
    kv_delete_RTT3_check(ctx);
}

void Client::kv_delete_RTT4_PL(KVReqCtx * ctx){
    DeleteCtx *dctx = ctx->dctx;
    RaceHashSlot * new_local_slot_ptr;
    uint64_t local_com_bucket_addr; 
    uint64_t old_local_slot_addr; 
    uint64_t remote_slot_addr;
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    ctx->is_finished = false;
    int bucket_idx, slot_idx;
    KVTableAddrInfo *addr_info = dctx->PL_addr_info;
    BucketBuffer *bucket_buffer;
    for(int i = 0;i < m;i ++){
        bucket_buffer = &dctx->PL_bucket_buffer[i];
        get_bucket_info(bucket_buffer);
        if(i == 0){
            find_empty_slot_from_bucket_buffer(bucket_buffer, addr_info);
            bucket_idx = bucket_buffer->bucket_idx;
            slot_idx   = bucket_buffer->slot_idx;
        }
        if (bucket_idx == -1) {
            RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " << "delete workflow:find ("<< i << ") parity logging buckets failed";
            ctx->is_finished = true;
            ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
            return;
        }
        new_local_slot_ptr = (RaceHashSlot *)bucket_buffer->local_cas_target_value_addr;
        fill_slot(&dctx->mm_alloc_parity_logging_ctx[i], &dctx->PL_hash_info[i], new_local_slot_ptr); 
        if (bucket_idx < 2) {
            local_com_bucket_addr = (uint64_t)bucket_buffer->f_com_bucket; 
            old_local_slot_addr = (uint64_t)&(bucket_buffer->f_com_bucket[bucket_idx].slots[slot_idx]);
            remote_slot_addr = addr_info->f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
        } else {
            local_com_bucket_addr = (uint64_t)bucket_buffer->s_com_bucket; 
            old_local_slot_addr = (uint64_t)&(bucket_buffer->s_com_bucket[bucket_idx - 2].slots[slot_idx]); 
            remote_slot_addr = addr_info->s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr); 
        }
        if(i == 0){
            fill_cas_slot_through_bucket_buffer_value(&bucket_buffer->kv_modify_pr_cas_list[0],
                addr_info->server_id_list[i], local_buf_mr_->lkey, remote_slot_addr, 
                (uint64_t)bucket_buffer->local_cas_return_value_addr,
                *(uint64_t *)old_local_slot_addr, *(uint64_t *)new_local_slot_ptr
                );
            sr_list = gen_cas_sr_lists_add_id(ctx->coro_id, 
                bucket_buffer->kv_modify_pr_cas_list, 
                &sr_list_num, DELETE_CAS_PL_SLOT);
        } else {
            sr_list = gen_one_sr_list((uint64_t)new_local_slot_ptr, sizeof(uint64_t), local_buf_mr_->lkey, 
                ctx->coro_id, addr_info->server_id_list[i], DELETE_WRITE_PL_SLOT, i, IBV_WR_RDMA_WRITE,
                remote_slot_addr, 0, NULL);
        }
        kv_delete_send(ctx->dctx, sr_list, 1);
    }
}

void Client::kv_delete_RTT4_kv(KVReqCtx * ctx){
    DeleteCtx *dctx = ctx->dctx;
    int32_t bucket_idx = ctx->bucket_idx;
    int32_t slot_idx   = ctx->slot_idx;
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
        IBV_SEND_SIGNALED, NULL);
    kv_delete_send(dctx, sr_list, 1);
    CasKvMessage *mes = &dctx->cas_rec_ele->cas_kv_mes;
    mes->new_value = new_value;
    mes->remote_slot_addr = remote_slot_addr;
    mes->server_id = ctx->tbl_addr_info.server_id_list[0];
    mes->local_addr = (uint64_t)ctx->local_cas_return_value_addr;
}

void Client::kv_delete_RTT4_kv_meta(KVReqCtx *ctx){
    IbvSrList *sr_list;
    *(uint64_t *)ctx->local_bucket_addr = 0;
    int bucket_idx = ctx->bucket_idx;
    int slot_idx   = ctx->slot_idx;
    uint64_t sub = KV_BUCKET_SIZE * bucket_idx + sizeof(KvEcMetaSlot) * slot_idx + sizeof(uint64_t) * 3;
    uint64_t remote_slot_addr; 
    for(int i = 0;i < m + 1;i ++){
        remote_slot_addr = ctx->tbl_addr_info.f_bucket_addr[i] + sub;
        sr_list = gen_sr_list((uint64_t)ctx->local_bucket_addr, sizeof(EcMetaSlot),
            local_buf_mr_->lkey, ctx->coro_id, ctx->tbl_addr_info.server_id_list[i],
            DELETE_WRITE_KV_META, i, IBV_WR_RDMA_WRITE, remote_slot_addr);
        kv_delete_send(ctx->dctx, sr_list, 1);
    }
}

void Client::kv_delete_check_write_PL_and_write_kv(KVReqCtx * ctx){
    DeleteCtx *dctx = ctx->dctx;
    if (*(uint64_t *)ctx->local_cas_return_value_addr == 
        ctx->old_value) {
        ctx->ret_val.ret_code = KV_OPS_SUCCESS;
        ctx->is_finished = true;
    } else {
        // print_error("update workflow:RTT4 cas kv slot failed~");
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        ctx->is_finished = true;
        code_num_count.cache_hit_but_failed ++;
        dctx->cas_rec_ele->recovery_times = CAS_FAILED_RECOVERY_TIMES;
        dctx->cas_rec_ele->recovery_when = rand() % RAND_RECOVERY_TIMES;
        cas_rec->insert(dctx->coro_id, dctx->cas_rec_ele);
    }
}

void Client::kv_delete_RTT4_sync(KVReqCtx * ctx) {
    kv_delete_init_sr_list(ctx->dctx);
    kv_delete_RTT4_PL(ctx);
    if(ctx->is_finished == true){
        cout << "cas PL failed~" << endl;
        return;
    }
    kv_delete_RTT4_kv(ctx);
    kv_delete_RTT4_kv_meta(ctx);
    nm_->rdma_post_check_sr_list(ctx->dctx->comp_wrid_map);
    kv_delete_check_write_PL_and_write_kv(ctx);
}

void Client::kv_delete_RTT4_async(KVReqCtx * ctx) {
    kv_delete_init_sr_list(ctx->dctx);
    kv_delete_RTT4_PL(ctx);
    if(ctx->is_finished == true){
        cout << "cas PL failed~" << endl;
        return;
    }
    kv_delete_RTT4_kv(ctx);
    kv_delete_RTT4_kv_meta(ctx);
    kv_delete_post_and_wait(ctx->dctx);
    kv_delete_check_write_PL_and_write_kv(ctx);
}

void Client::kv_delete_RTT5_log(DeleteCtx *dctx){
    make_log_entry((LogEntry *)dctx->stripe_meta_end_log_buf_addr,
        LOG_DELETE_END, dctx->stripe_id);
    IbvSrList * sr_list;
    uint8_t server_id = dctx->stripe_meta_addr_info->server_id_list[0];
    sr_list = gen_sr_list(dctx->stripe_meta_end_log_buf_addr,
        LOG_ENTRY_LEN, local_buf_mr_->lkey, dctx->coro_id, server_id, DELETE_LOG_END,
        0, IBV_WR_RDMA_WRITE, get_log_entry_addr(server_id));
    kv_delete_send(dctx, sr_list, 1);
}

void Client::kv_delete_RTT5_sync(KVReqCtx * ctx){
    kv_delete_init_sr_list(ctx->dctx);
    kv_delete_cas_stripe_meta_second(ctx->dctx);
    // kv_delete_RTT5_log(ctx->dctx);
    nm_->rdma_post_check_sr_list(ctx->dctx->comp_wrid_map);
}

void Client::kv_delete_RTT5_async(KVReqCtx * ctx){
    kv_delete_init_sr_list(ctx->dctx);
    kv_delete_cas_stripe_meta_second(ctx->dctx);
    kv_delete_RTT5_log(ctx->dctx);
    kv_delete_post_and_wait(ctx->dctx);
}

int Client::kv_delete(KVReqCtx * ctx) {
    kv_delete_RTT1_async(ctx);
    kv_delete_RTT2_async(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.ret_code;
    }
    kv_delete_RTT3_async(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.ret_code;
    }
    kv_delete_RTT4_async(ctx);
    kv_delete_RTT5_async(ctx);
    return ctx->ret_val.ret_code;
}

void Client::hash_compute_fp(KVHashInfo * hash_info){
    hash_info->fp = hash_index_compute_fp(hash_info->hash_value);
}

IbvSrList * Client::gen_bucket_sr_lists(int coro_id, KVTableAddrInfo *tbl_addr_info, 
    BucketBuffer *bucket_buffer, __OUT uint32_t * num_sr_lists, int wr_add){
    IbvSrList * ret = get_one_sr_list();
    struct ibv_send_wr * sr = get_double_sr();
    struct ibv_sge * sge = get_double_sge();
    sge[0].addr   = (uint64_t)bucket_buffer->local_bucket_addr;
    sge[0].length = 2 * sizeof(RaceHashBucket);
    sge[0].lkey   = local_buf_mr_->lkey;
    sge[1].addr   = (uint64_t)bucket_buffer->local_bucket_addr + 2 * sizeof(RaceHashBucket);
    sge[1].length = 2 * sizeof(RaceHashBucket);
    sge[1].lkey   = local_buf_mr_->lkey;
    sr[0].wr_id   = ib_gen_wr_id(coro_id, tbl_addr_info->server_id_list[0], wr_add, 0);
    sr[0].sg_list = &sge[0];
    sr[0].num_sge = 1;
    sr[0].opcode  = IBV_WR_RDMA_READ;
    sr[0].wr.rdma.remote_addr = tbl_addr_info->f_bucket_addr[0];
    sr[0].wr.rdma.rkey        = tbl_addr_info->f_bucket_addr_rkey[0];
    sr[0].send_flags = 0;
    sr[0].next    = &sr[1];
    sr[1].wr_id   = ib_gen_wr_id(coro_id, tbl_addr_info->server_id_list[0], wr_add, 1);
    sr[1].sg_list = &sge[1];
    sr[1].num_sge = 1;
    sr[1].opcode  = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = tbl_addr_info->s_bucket_addr[0];
    sr[1].wr.rdma.rkey        = tbl_addr_info->s_bucket_addr_rkey[0];
    sr[1].send_flags = 0;
    sr[1].next    = NULL;
    ret->num_sr = 2;
    ret->sr_list = sr;
    ret->server_id = tbl_addr_info->server_id_list[0];
    *num_sr_lists = 1;
    return ret;
}

IbvSrList * Client::gen_bucket_sr_lists_big(int coro_id, KVTableAddrInfo *tbl_addr_info, 
    BucketBuffer *bucket_buffer, __OUT uint32_t * num_sr_lists, int wr_add){
    IbvSrList * ret = get_one_sr_list();
    struct ibv_send_wr * sr = get_double_sr();
    struct ibv_sge * sge = get_double_sge();
    sge[0].addr   = (uint64_t)bucket_buffer->local_bucket_addr;
    sge[0].length = 2 * sizeof(KvEcMetaBucket);
    sge[0].lkey   = local_buf_mr_->lkey;
    sge[1].addr   = (uint64_t)bucket_buffer->local_bucket_addr + 2 * sizeof(KvEcMetaBucket);
    sge[1].length = 2 * sizeof(KvEcMetaBucket);
    sge[1].lkey   = local_buf_mr_->lkey;
    sr[0].wr_id   = ib_gen_wr_id(coro_id, tbl_addr_info->server_id_list[0], wr_add, 0);
    sr[0].sg_list = &sge[0];
    sr[0].num_sge = 1;
    sr[0].opcode  = IBV_WR_RDMA_READ;
    sr[0].wr.rdma.remote_addr = tbl_addr_info->f_bucket_addr[0];
    sr[0].wr.rdma.rkey        = tbl_addr_info->f_bucket_addr_rkey[0];
    sr[0].send_flags = 0;
    sr[0].next    = &sr[1];
    sr[1].wr_id   = ib_gen_wr_id(coro_id, tbl_addr_info->server_id_list[0], wr_add, 1);
    sr[1].sg_list = &sge[1];
    sr[1].num_sge = 1;
    sr[1].opcode  = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = tbl_addr_info->s_bucket_addr[0];
    sr[1].wr.rdma.rkey        = tbl_addr_info->s_bucket_addr_rkey[0];
    sr[1].send_flags = 0;
    sr[1].next    = NULL;
    ret->num_sr = 2;
    ret->sr_list = sr;
    ret->server_id = tbl_addr_info->server_id_list[0];
    *num_sr_lists = 1;
    return ret;
}

IbvSrList * Client::gen_bucket_sr_lists_rep(int coro_id, KVTableAddrInfo *tbl_addr_info, 
    BucketBuffer *bucket_buffer, int wr_add, int rep_id){
    IbvSrList * ret = get_one_sr_list();
    struct ibv_send_wr * sr = get_double_sr();
    struct ibv_sge * sge = get_double_sge();
    sge[0].addr   = (uint64_t)bucket_buffer->local_bucket_addr;
    sge[0].length = 2 * sizeof(RaceHashBucket);
    sge[0].lkey   = local_buf_mr_->lkey;
    sge[1].addr   = (uint64_t)bucket_buffer->local_bucket_addr + 2 * sizeof(RaceHashBucket);
    sge[1].length = 2 * sizeof(RaceHashBucket);
    sge[1].lkey   = local_buf_mr_->lkey;
    sr[0].wr_id   = ib_gen_wr_id(coro_id, tbl_addr_info->server_id_list[rep_id], wr_add, 0);
    sr[0].sg_list = &sge[0];
    sr[0].num_sge = 1;
    sr[0].opcode  = IBV_WR_RDMA_READ;
    sr[0].wr.rdma.remote_addr = tbl_addr_info->f_bucket_addr[rep_id];
    sr[0].wr.rdma.rkey        = tbl_addr_info->f_bucket_addr_rkey[rep_id];
    sr[0].send_flags = 0;
    sr[0].next    = &sr[1];
    sr[1].wr_id   = ib_gen_wr_id(coro_id, tbl_addr_info->server_id_list[rep_id], wr_add, 1);
    sr[1].sg_list = &sge[1];
    sr[1].num_sge = 1;
    sr[1].opcode  = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = tbl_addr_info->s_bucket_addr[rep_id];
    sr[1].wr.rdma.rkey        = tbl_addr_info->s_bucket_addr_rkey[rep_id];
    sr[1].send_flags = 0;
    sr[1].next    = NULL;
    ret->num_sr = 2;
    ret->sr_list = sr;
    ret->server_id = tbl_addr_info->server_id_list[rep_id];
    return ret;
}

IbvSrList * Client::gen_bucket_sr_lists_parity(int coro_id, KVTableAddrInfo *tbl_addr_info, 
    BucketBuffer *bucket_buffer, __OUT uint32_t * num_sr_lists, int wr_add, int lkey){
    IbvSrList * ret = get_one_sr_list_encoding();
    struct ibv_send_wr * sr = get_double_sr_encoding();
    struct ibv_sge * sge = get_double_sge_encoding();
    sge[0].addr   = (uint64_t)bucket_buffer->local_bucket_addr;
    sge[0].length = 2 * sizeof(KvEcMetaBucket);
    sge[0].lkey   = lkey;
    sge[1].addr   = (uint64_t)bucket_buffer->local_bucket_addr + 2 * sizeof(KvEcMetaBucket);
    sge[1].length = 2 * sizeof(KvEcMetaBucket);
    sge[1].lkey   = lkey;
    sr[0].wr_id   = ib_gen_wr_id(coro_id, tbl_addr_info->server_id_list[0], wr_add, 0);
    sr[0].sg_list = &sge[0];
    sr[0].num_sge = 1;
    sr[0].opcode  = IBV_WR_RDMA_READ;
    sr[0].wr.rdma.remote_addr = tbl_addr_info->f_bucket_addr[0];
    sr[0].wr.rdma.rkey        = tbl_addr_info->f_bucket_addr_rkey[0];
    sr[0].next    = &sr[1];
    sr[1].wr_id   = ib_gen_wr_id(coro_id, tbl_addr_info->server_id_list[0], wr_add, 1);
    sr[1].sg_list = &sge[1];
    sr[1].num_sge = 1;
    sr[1].opcode  = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = tbl_addr_info->s_bucket_addr[0];
    sr[1].wr.rdma.rkey        = tbl_addr_info->s_bucket_addr_rkey[0];
    sr[1].next    = NULL;
    ret->num_sr = 2;
    ret->sr_list = sr;
    ret->server_id = tbl_addr_info->server_id_list[0];
    *num_sr_lists = 1;
    return ret;
}

IbvSrList * Client::gen_bucket_sr_lists_metadata(int coro_id, KVTableAddrInfo *tbl_addr_info, 
    BucketBuffer *bucket_buffer, __OUT uint32_t * num_sr_lists, int wr_add, int lkey){
    IbvSrList * ret = get_one_sr_list_encoding();
    struct ibv_send_wr * sr = get_double_sr_encoding();
    struct ibv_sge * sge = get_double_sge_encoding();
    sge[0].addr   = (uint64_t)bucket_buffer->local_bucket_addr;
    sge[0].length = 2 * sizeof(RaceHashBucket);
    sge[0].lkey   = lkey;
    sge[1].addr   = (uint64_t)bucket_buffer->local_bucket_addr + 2 * sizeof(RaceHashBucket);
    sge[1].length = 2 * sizeof(RaceHashBucket);
    sge[1].lkey   = lkey;
    sr[0].wr_id   = ib_gen_wr_id(coro_id, tbl_addr_info->server_id_list[0], wr_add, 0);
    sr[0].sg_list = &sge[0];
    sr[0].num_sge = 1;
    sr[0].opcode  = IBV_WR_RDMA_READ;
    sr[0].wr.rdma.remote_addr = tbl_addr_info->f_bucket_addr[0];
    sr[0].wr.rdma.rkey        = tbl_addr_info->f_bucket_addr_rkey[0];
    sr[0].next    = &sr[1];
    sr[1].wr_id   = ib_gen_wr_id(coro_id, tbl_addr_info->server_id_list[0], wr_add, 1);
    sr[1].sg_list = &sge[1];
    sr[1].num_sge = 1;
    sr[1].opcode  = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = tbl_addr_info->s_bucket_addr[0];
    sr[1].wr.rdma.rkey        = tbl_addr_info->s_bucket_addr_rkey[0];
    sr[1].next    = NULL;
    ret->num_sr = 2;
    ret->sr_list = sr;
    ret->server_id = tbl_addr_info->server_id_list[0];
    *num_sr_lists = 1;
    return ret;
}

void Client::kv_degread_read_kv_meta(KVReqCtx * ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    sr_list = gen_sr_list((uint64_t)drctx->kv_meta_bucket_buffer->local_bucket_addr, COMBINED_BUCKET_SIZE, 
        local_buf_mr_->lkey, drctx->coro_id, ctx->tbl_addr_info.server_id_list[1], DEGREAD_KV_META,
        0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[1]);
    send_one_sr_list(sr_list, drctx->comp_wrid_map);
}

void Client::kv_degread_init_sr_list(KVReqCtx * ctx){
    ctx->drctx->sr_list_batch.clear();
    ctx->drctx->sr_list_num_batch.clear();
    ctx->drctx->comp_wrid_map->clear();
}

int Client::kv_degrade_send_sr_list(KVReqCtx * ctx){
    int ret = 0;
    struct ibv_wc wc;
    ret = nm_->rdma_post_sr_list_batch_sync(ctx->drctx->sr_list_batch, 
        ctx->drctx->sr_list_num_batch, &wc);
    return ret;
}

void Client::kv_degread_RTT1_sync(KVReqCtx * ctx){
    int ret;
    kv_degread_init_sr_list(ctx);
    kv_degread_read_kv_meta(ctx);
    ret = nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
    if(ret == -1){
        print_error("degrade read workflow:RTT1 send sr list failed~");
        exit(0);
    }
}

void Client::kv_degread_RTT1_async(KVReqCtx * ctx){
    kv_degread_init_sr_list(ctx);
    kv_degread_read_kv_meta(ctx);
    poll_completion(*ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_read_kv_index_bd(KVReqCtx *ctx){
    kv_degread_read_kv_meta(ctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_RTT1_bd(KVReqCtx * ctx){
    kv_degread_init_sr_list(ctx);
    kv_degread_read_kv_index_bd(ctx);
}

void Client::kv_degrade_read_analysis_kv_meta(KVReqCtx * ctx){
    get_bucket_info(ctx->drctx->kv_meta_bucket_buffer);
    int if_find;
    ctx->is_finished = false;
    if_find = find_kv_meta_slot((BucketBuffer *)ctx->drctx->kv_meta_bucket_buffer->local_bucket_addr,
        ctx->drctx->kv_meta_ctx, ctx->hash_info.fp);
    if(if_find == 0){
        // print_error("degrade read workflow:analysis_kv_meta failed~");
        ctx->is_finished = true;
        ctx->ret_val.value_addr = NULL;
        return;
    }
    ctx->drctx->stripe_id = ctx->drctx->kv_meta_ctx->sid;
    ctx->drctx->parity_id[0] = ctx->drctx->kv_meta_ctx->pid1;
}

void Client::kv_degread_metadata_hash_info(KVReqCtx * ctx){
    uint64_t key, hash_key;
    for(int i = 0;i < k_m;i ++){
        key = connect_metadata_key_to_64int(ctx->drctx->kv_meta_ctx->sid, i);
        hash_key = hash_64int_to_64int(key);
        ctx->drctx->metadata_hash_info[i].hash_value = hash_key;
        hash_compute_fp(&ctx->drctx->metadata_hash_info[i]);
    }
}

void Client::kv_degrade_metadata_addr_info(KVReqCtx * ctx){
    get_server_subtable_metadata(ctx->drctx->stripe_id, ctx->drctx->metadata_hash_info,
        ctx->drctx->metadata_addr_info);
}

void Client::kv_degread_prepare_metadata_bucket(KVReqCtx * ctx){
    kv_degread_metadata_hash_info(ctx);
    kv_degrade_metadata_addr_info(ctx);
}
    
void Client::kv_degread_metadata_bucket(KVReqCtx * ctx){
    kv_degread_prepare_metadata_bucket(ctx);
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    for(int i = 0;i < k;i ++){
        sr_list = gen_bucket_sr_lists_rep(ctx->coro_id, ctx->drctx->metadata_addr_info,
            &ctx->drctx->metadata_bucket_buffer[i], i * 2, i);
        send_one_sr_list(sr_list, ctx->drctx->comp_wrid_map);
    }
}

void Client::get_bucket_index(KVHashInfo * hash_info, KVTableAddrInfo *addr_info){
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

void Client::get_two_combined_bucket_index(KVHashInfo * hash_info, KVTableAddrInfo *addr_info){
    uint64_t hash_value = hash_info->hash_value;
    uint64_t f_index_value = subtable_first_index(hash_value, RACE_HASH_ADDRESSABLE_BUCKET_NUM);
    uint64_t s_index_value = subtable_second_index(hash_value, f_index_value, RACE_HASH_ADDRESSABLE_BUCKET_NUM);
    uint64_t f_idx, s_idx;
    if (f_index_value % 2 == 0) 
        f_idx = f_index_value / 2 * 3;
    else
        f_idx = f_index_value / 2 * 3 + 1;
    if (s_index_value % 2 == 0)
        s_idx = s_index_value / 2 * 3;
    else 
        s_idx = s_index_value / 2 * 3 + 1;
    addr_info->f_main_idx = f_index_value % 2;
    addr_info->s_main_idx = s_index_value % 2;
    addr_info->f_idx = f_idx;
    addr_info->s_idx = s_idx;
}

void Client::get_rep_server_subtable_addr(KVHashInfo * hash_info, KVTableAddrInfo *addr_info, uint8_t type){
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

void Client::get_PL_server_subtable_addr(KVHashInfo * hash_info, KVTableAddrInfo *addr_info, int *pid){
    vector<uint8_t> server_list;
    for(int i = 0;i < m;i ++){
        server_list.push_back(pid[i]);
    }
    int index = (*hashring_table->PL_map)[combined_server_id(server_list)];
    uint64_t subtable_addr;
    RaceHashSubtableEntry *subtable_entry = hashring_table->PL_hash_root_->subtable_entry[index].subtable_entry_PL;
    uint8_t server_id;
    for(int i = 0;i < m;i ++){
        subtable_addr = hash_index_convert_40_to_64_bits(subtable_entry[i].pointer);
        server_id = subtable_entry[i].server_id;
        addr_info->server_id_list[i]     = server_id;
        addr_info->f_bucket_addr[i]      = subtable_addr + addr_info->f_idx * RACE_BUCKET_SIZE;
        addr_info->f_bucket_addr_rkey[i] = server_mr_info_map_[server_id]->rkey;
        addr_info->s_bucket_addr[i]      = subtable_addr + addr_info->s_idx * RACE_BUCKET_SIZE;
        addr_info->s_bucket_addr_rkey[i] = server_mr_info_map_[server_id]->rkey;
    }
}

void Client::get_server_subtable_kv(KVHashInfo * hash_info, KVTableAddrInfo *addr_info){
    get_bucket_index(hash_info, addr_info);
    // get (m + 1)kv and kv ec meta server id;get (m + 1)subtable start addr
    get_rep_server_subtable_addr(hash_info, addr_info, KV_REP);
}

void Client::get_server_subtable_stripe_meta(KVHashInfo * hash_info, KVTableAddrInfo *addr_info){
    get_two_combined_bucket_index(hash_info, addr_info);
    get_rep_server_subtable_addr(hash_info, addr_info, STRIPE_META_REP);
}

void Client::kv_degrade_prepare_get_stripe_meta(KVReqCtx * ctx){
    prepare_stripe_meta(ctx->drctx->kv_meta_ctx->sid, ctx->drctx->stripe_meta_hash_info,
        ctx->drctx->stripe_meta_addr_info);
}

void Client::kv_degread_stripe_meta(KVReqCtx * ctx){
    kv_degrade_prepare_get_stripe_meta(ctx);
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    sr_list = gen_bucket_sr_lists(ctx->coro_id, ctx->drctx->stripe_meta_addr_info,
        ctx->drctx->stripe_meta_bucket_buffer, &sr_list_num,
        READ_STRIPE_META_BUCKET);
    send_one_sr_list(sr_list, ctx->drctx->comp_wrid_map);
}

void Client::kv_degrade_prepare_get_P0_bucket_hash_info(KVReqCtx * ctx){
    uint64_t key = connect_parity_key_to_64int(ctx->drctx->kv_meta_ctx->sid, 
        ctx->drctx->kv_meta_ctx->pid1);
    ctx->drctx->P0_hash_info->hash_value = hash_64int_to_64int(key);
    hash_compute_fp(ctx->drctx->P0_hash_info);
}

void Client::kv_degrade_prepare_get_P0_bucket_addr_info(KVReqCtx * ctx){
    get_server_subtable_parity(ctx->drctx->P0_hash_info, ctx->drctx->P0_addr_info,
        ctx->drctx->kv_meta_ctx->pid1);
}

void Client::kv_degread_prepare_get_P0_bucket(KVReqCtx * ctx){
    kv_degrade_prepare_get_P0_bucket_hash_info(ctx);
    kv_degrade_prepare_get_P0_bucket_addr_info(ctx);
}

void Client::kv_degread_get_P0_bucket(KVReqCtx * ctx){
    kv_degread_prepare_get_P0_bucket(ctx);
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    sr_list = gen_bucket_sr_lists_big(ctx->coro_id, ctx->drctx->P0_addr_info, 
        ctx->drctx->P0_bucket_buffer, &sr_list_num, READ_P0_BUCKET);
    send_one_sr_list(sr_list, ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_prepare_PL_bucket_hash_info(KVReqCtx * ctx){
    uint64_t key = connect_parity_key_to_64int(ctx->drctx->kv_meta_ctx->sid,
        ctx->drctx->kv_meta_ctx->pid1);
    ctx->drctx->PL_hash_info->hash_value = hash_64int_to_64int(key);
    hash_compute_fp(ctx->drctx->PL_hash_info);
}
    
void Client::kv_degread_prepare_PL_bucket_addr_info(KVReqCtx * ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    get_two_combined_bucket_index(drctx->PL_hash_info, drctx->PL_addr_info);
    get_PL_server_subtable_addr(drctx->PL_hash_info, drctx->PL_addr_info, drctx->parity_id);
}

void Client::kv_degread_prepare_get_PL_bucket(KVReqCtx * ctx){
    kv_degread_prepare_PL_bucket_hash_info(ctx);
    kv_degread_prepare_PL_bucket_addr_info(ctx);
}

void Client::kv_degread_get_PL_bucket(KVReqCtx * ctx){
    kv_degread_prepare_get_PL_bucket(ctx);
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    sr_list = gen_bucket_sr_lists(ctx->coro_id, ctx->drctx->PL_addr_info, 
        ctx->drctx->PL_bucket_buffer, &sr_list_num,
        READ_LOGGING_BUCKET);
    send_one_sr_list(sr_list, ctx->drctx->comp_wrid_map);
}

void Client::kv_degrade_read_RTT_2(KVReqCtx *ctx){
    kv_degread_stripe_meta(ctx);
    kv_degread_metadata_bucket(ctx);
    kv_degread_get_P0_bucket(ctx);
}

void Client::kv_degread_RTT2_sync(KVReqCtx * ctx){
    int ret;
    kv_degread_init_sr_list(ctx);
    kv_degrade_read_analysis_kv_meta(ctx);
    if(ctx->is_finished == true){
        return;
    }
    kv_degrade_read_RTT_2(ctx);
    ret = nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
    if(ret == -1){
        print_error("degrade workflow:read_get_stripe_meta_get_ec_meta_get_P0 send sr list failed~");
        exit(0);
    }
}

void Client::kv_degread_RTT2_async(KVReqCtx * ctx){
    kv_degread_init_sr_list(ctx);
    kv_degrade_read_analysis_kv_meta(ctx);
    if(ctx->is_finished == true){
        return;
    }
    kv_degrade_read_RTT_2(ctx);
    poll_completion(*ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_read_metadata_index_bd(KVReqCtx *ctx){
    kv_degread_metadata_bucket(ctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_read_P0_index_bd(KVReqCtx *ctx){
    kv_degread_get_P0_bucket(ctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_read_stripe_meta_bd(KVReqCtx *ctx){
    kv_degread_stripe_meta(ctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_RTT2_bd(KVReqCtx * ctx){
    kv_degrade_read_analysis_kv_meta(ctx);
    if(ctx->is_finished == true){
        return;
    }
    kv_degread_read_metadata_index_bd(ctx);
    kv_degread_read_P0_index_bd(ctx);
    kv_degread_read_stripe_meta_bd(ctx);
}

int Client::race_find_bucket_no_idx(RaceHashSlot *slot[4], KVHashInfo *hash_info, 
    uint64_t *local_buf_addr, uint32_t lkey, vector<KVRWAddr> & read_addr_list){
    KVRWAddr cur_kv_addr;
    int if_find = 0;
    for(int i = 0;i < 4;i ++){
        for(int j = 0;j < RACE_HASH_ASSOC_NUM; j++){
            if(slot[i][j].fp == hash_info->fp && slot[i][j].kv_len != 0){
                cur_kv_addr.r_kv_addr = hash_index_convert_40_to_64_bits(slot[i][j].pointer);
                cur_kv_addr.rkey = server_mr_info_map_[slot[i][j].server_id]->rkey;
                cur_kv_addr.l_kv_addr = *local_buf_addr;
                cur_kv_addr.lkey = lkey;
                cur_kv_addr.length = slot[i][j].kv_len * mm_->subblock_sz_;
                cur_kv_addr.server_id = slot[i][j].server_id;
                read_addr_list.push_back(cur_kv_addr);
                *local_buf_addr += cur_kv_addr.length;
                if_find ++;
            }
        }
    }
    return if_find;
}

bool Client::find_P0_slot(void *bucket, uint8_t fp, uint64_t *local_addr, vector<KVRWAddr> & read_addr_list, 
    vector<std::pair<int32_t, int32_t>> & idx_list){
    KVRWAddr cur_kv_addr;
    bool if_find = false;
    KvEcMetaBucket *pre_bucket = (KvEcMetaBucket *)bucket;
    RaceHashSlot *slot;
    for(int i = 0;i < 4;i ++){
        for(int j = 0;j < RACE_HASH_ASSOC_NUM; j++){
            slot = &pre_bucket[i].slots[j].kv_slot;
            if(slot->fp == fp && slot->kv_len * mm_->subblock_sz_ == 0){
                cur_kv_addr.r_kv_addr = hash_index_convert_40_to_64_bits(slot->pointer);
                cur_kv_addr.rkey = server_mr_info_map_[slot->server_id]->rkey;
                cur_kv_addr.l_kv_addr = *local_addr;
                cur_kv_addr.lkey = local_buf_mr_->lkey;
                cur_kv_addr.length = buffer->block_size;
                cur_kv_addr.server_id = slot->server_id;
                read_addr_list.push_back(cur_kv_addr);
                idx_list.push_back(make_pair(i, j));
                *local_addr += buffer->block_size;
                if_find = true;
            }
        }
    }
    return if_find;
}

void Client::kv_degread_analysis_metadata_bucket(KVReqCtx * ctx){
    uint64_t local_ec_meta_buf_addr = (uint64_t)ctx->drctx->ec_meta_buf_;
    int if_find;
    ctx->is_finished = false;
    for(int i = 0;i < k;i ++){
        get_bucket_info(&ctx->drctx->metadata_bucket_buffer[i]);
        ctx->drctx->kv_read_metadata_addr_list[i].clear();
        if(i == ctx->drctx->kv_meta_ctx->cid){
            ctx->drctx->pre_kv_meta = local_ec_meta_buf_addr;
        }
        if_find = race_find_bucket_no_idx(ctx->drctx->metadata_bucket_buffer[i].slot_arr, 
            &ctx->drctx->metadata_hash_info[i], &local_ec_meta_buf_addr, 
            local_buf_mr_->lkey, ctx->drctx->kv_read_metadata_addr_list[i]);
        if(if_find == 0){
            // print_error("degrade read workflow:analysis_ec_meta_bucket failed~");
            ctx->is_finished = true;
            ctx->ret_val.value_addr = NULL;
            return;
        } else if(if_find > 1){
            // print_error("degrade read workflow:analysis_ec_meta_bucket match more~");
            ctx->is_finished = true;
            ctx->ret_val.value_addr = NULL;
            return;
        }
        if(i == ctx->drctx->kv_meta_ctx->cid){
            ctx->drctx->kv_read_metadata_addr_list[i].front().length = BIT_MAP_SIZE;
        }
    }
}

void Client::kv_degread_get_metadata(KVReqCtx * ctx){
    kv_degread_analysis_metadata_bucket(ctx);
    if(ctx->is_finished == true){
        return;
    }
    IbvSrList * sr_list;
    for(int i = 0;i < k;i ++){
        sr_list = gen_read_value_sr_lists(ctx->coro_id, DEGREAD_READ_METADATA + i,
            ctx->drctx->kv_read_metadata_addr_list[i][0]);
        send_one_sr_list(sr_list, ctx->drctx->comp_wrid_map);
    }
}

bool Client::find_stripe_meta_slot_struct(BucketBuffer * bucketbuffer, 
    StripeMetaCtx *stripe_meta_ctx, int stripe_id, BucketSlotIdx *bucket_slot_idx){
    bool if_find = false;
    StripeMetaSlot *slot;
    int tf, index;
    for(int i = 0;i < 4;i ++){
        index = find_seq[i];
        for(int j = RACE_HASH_ASSOC_NUM - 1;j >= 0;j --){
            slot = (StripeMetaSlot *)&bucketbuffer->slot_arr[index][j];
            if(get_stripe_meta_stripe_id(slot) == stripe_id){
                get_stripe_meta_slot(slot, stripe_meta_ctx);
                bucket_slot_idx->bucket_idx = index;
                bucket_slot_idx->slot_idx = j;
                bucket_slot_idx->slot_value = *(uint64_t *)slot;
                if_find = true;
                return if_find;
            }
        }
    }
    return if_find;
}

void Client::kv_degread_analysis_stripe_meta(DegradeReadCtx * drctx){
    drctx->is_finished = false;
    get_bucket_info(drctx->stripe_meta_bucket_buffer);
    bool if_find = false;
    if_find = find_stripe_meta_slot_struct(drctx->stripe_meta_bucket_buffer,
        drctx->stripe_meta_ctx, drctx->kv_meta_ctx->sid, drctx->stripe_meta_idx);
    if(if_find == false){ 
        // print_error("degrade read workflow:analysis stripe meta bucket:find slot failed~");
        drctx->is_finished = true;
        drctx->ret_val.value_addr = NULL;
        return;
    }
    if(drctx->stripe_meta_ctx->RL == pow(2, 2) - 1){
        // print_error("degrade read workflow:analysis stripe meta bucket:find rl is full~");
        drctx->is_finished = true;
        drctx->ret_val.value_addr = NULL;
        return;
    }
    if(drctx->stripe_meta_ctx->RL == 0){
        drctx->is_cas = true;
    } else {
        drctx->is_cas = false;
    }
    if(drctx->stripe_meta_ctx->WL != 0){
        print_error("degrade read workflow:analysis stripe meta bucket:find wl is not 0(there is update doing)~");
        drctx->is_finished = true;
        drctx->ret_val.value_addr = NULL;
        return;
    }

    for(int i = 0;i < m - 1;i ++){
        drctx->parity_id[i + 1] = drctx->stripe_meta_ctx->pid[i];
    }
}

void Client::fill_cas_slot_through_bucket_buffer_value(KVCASAddr * cur_cas_addr, 
    int server_id, uint32_t lkey, uint64_t remote_slot_addr, uint64_t return_local_slot_addr,
    uint64_t old_value, uint64_t new_value){
    cur_cas_addr->r_kv_addr = remote_slot_addr;
    cur_cas_addr->rkey = server_mr_info_map_[server_id]->rkey;
    cur_cas_addr->l_kv_addr = return_local_slot_addr;
    cur_cas_addr->lkey = lkey;
    cur_cas_addr->orig_value = old_value;
    cur_cas_addr->swap_value = new_value;
    cur_cas_addr->server_id = server_id;
}

void Client::kv_degread_write_stripe_meta_log(DegradeReadCtx *drctx){
    uint8_t log_type;
    uint32_t wr_id_type;
    if(drctx->is_cas == true){
        log_type = LOG_DEGRADE_CAS_STRIPE_META;
        wr_id_type = DEGRADE_CAS_STRIPE_META_LOG;
    } else {
        log_type = LOG_DEGRADE_FAA_STRIPE_META;
        wr_id_type = DEGRADE_FAA_STRIPE_META_LOG;
    }
    uint8_t server_id = drctx->stripe_meta_addr_info->server_id_list[0];
    make_log_entry((LogEntry *)drctx->stripe_meta_log_buf_addr, log_type, 
        drctx->stripe_meta_idx->slot_value);
    IbvSrList * sr_list;
    sr_list = gen_sr_list(drctx->stripe_meta_log_buf_addr,
        LOG_ENTRY_LEN, local_buf_mr_->lkey, drctx->coro_id, server_id, wr_id_type,
        0, IBV_WR_RDMA_WRITE, get_log_entry_addr(server_id));
    send_one_sr_list(sr_list, drctx->comp_wrid_map);
}

void Client::kv_degread_make_cas_stripe_meta(DegradeReadCtx *drctx){
    int bucket_idx = drctx->stripe_meta_idx->bucket_idx;
    int slot_idx   = drctx->stripe_meta_idx->slot_idx;
    uint64_t local_com_bucket_addr, old_local_slot_addr, remote_slot_addr;
    if(bucket_idx < 2){
        local_com_bucket_addr = (uint64_t)drctx->stripe_meta_bucket_buffer->f_com_bucket;
        old_local_slot_addr = (uint64_t)&drctx->stripe_meta_bucket_buffer->
            f_com_bucket[bucket_idx].slots[slot_idx];
        remote_slot_addr = drctx->stripe_meta_addr_info->f_bucket_addr[0] +
            (old_local_slot_addr - local_com_bucket_addr);
    } else {
        local_com_bucket_addr = (uint64_t)drctx->stripe_meta_bucket_buffer->s_com_bucket;
        old_local_slot_addr = (uint64_t)&drctx->stripe_meta_bucket_buffer->
            s_com_bucket[bucket_idx - 2].slots[slot_idx];
        remote_slot_addr = drctx->stripe_meta_addr_info->s_bucket_addr[0] +
            (old_local_slot_addr - local_com_bucket_addr);
    }
    uint64_t old_value = *(uint64_t *)old_local_slot_addr;
    StripeMetaSlot *stripe_meta = (StripeMetaSlot *)old_local_slot_addr;
    write_stripe_meta_rl(stripe_meta, 1);
    uint64_t new_value = *(uint64_t *)old_local_slot_addr;
    fill_cas_slot_through_bucket_buffer_value(
        &drctx->stripe_meta_bucket_buffer->kv_modify_pr_cas_list[0],
        drctx->stripe_meta_addr_info->server_id_list[0],
        local_buf_mr_->lkey, remote_slot_addr,
        (uint64_t)drctx->stripe_meta_bucket_buffer->local_cas_return_value_addr,
        old_value, new_value
    );
}

void Client::kv_degread_cas_stripe_meta(DegradeReadCtx *drctx, bool if_add){
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    if(if_add == false){
        sr_list = gen_faa_sr_lists(drctx->coro_id,
            drctx->stripe_meta_bucket_buffer->kv_modify_pr_cas_list,
            &sr_list_num, -faa_need_value_RL);
        *(uint64_t *)drctx->stripe_meta_bucket_buffer->local_cas_return_value_addr = 0;
        send_one_sr_list(sr_list, drctx->comp_wrid_map);
        return;
    }
    kv_degread_make_cas_stripe_meta(drctx);
    if(drctx->is_cas == true){
        sr_list = gen_cas_sr_lists(drctx->coro_id,
            drctx->stripe_meta_bucket_buffer->kv_modify_pr_cas_list,
            &sr_list_num);
    } else {
        sr_list = gen_faa_sr_lists(drctx->coro_id,
            drctx->stripe_meta_bucket_buffer->kv_modify_pr_cas_list,
            &sr_list_num, faa_need_value_RL);
    }
    send_one_sr_list(sr_list, drctx->comp_wrid_map);
}

void Client::kv_degread_cas_stripe_meta_first(DegradeReadCtx *drctx){
    kv_degread_analysis_stripe_meta(drctx);
    if(drctx->is_finished == true){
        return;
    }
    // kv_degread_write_stripe_meta_log(drctx);
    kv_degread_cas_stripe_meta(drctx, true);
}

void Client::kv_degread_RTT3_sync(KVReqCtx * ctx){
    int ret;
    kv_degread_init_sr_list(ctx);
    kv_degread_get_metadata(ctx);
    if(ctx->is_finished == true){
        return;
    }
    kv_degread_get_PL_bucket(ctx);
    kv_degread_cas_stripe_meta_first(ctx->drctx);
    if(ctx->drctx->is_finished == true){
        ctx->is_finished = true;
        ctx->ret_val.value_addr = ctx->drctx->ret_val.value_addr;
        return;
    }
    ret = nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
    if(ret == -1){
        print_error("degrade read workflow:RTT3 send sr list failed");
        exit(0);
    }
    kv_degread_RTT3_check(ctx);
}

void Client::kv_degread_RTT3_async(KVReqCtx * ctx){
    kv_degread_init_sr_list(ctx);
    kv_degread_get_metadata(ctx);
    if(ctx->is_finished == true){
        return;
    }
    kv_degread_get_PL_bucket(ctx);
    kv_degread_cas_stripe_meta_first(ctx->drctx);
    if(ctx->drctx->is_finished == true){
        ctx->is_finished = true;
        ctx->ret_val.value_addr = ctx->drctx->ret_val.value_addr;
        return;
    }
    poll_completion(*ctx->drctx->comp_wrid_map);
    kv_degread_RTT3_check(ctx);
}

void Client::kv_degread_cas_stripe_meta_bd(KVReqCtx *ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    kv_degread_analysis_stripe_meta(drctx);
    if(drctx->is_finished == true){
        return;
    }
    kv_degread_cas_stripe_meta(drctx, true);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_read_metadata_bd(KVReqCtx *ctx){
    kv_degread_get_metadata(ctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_read_PL_index_bd(KVReqCtx *ctx){
    kv_degread_get_PL_bucket(ctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_write_stripe_log_bd(KVReqCtx *ctx){
    kv_degread_write_stripe_meta_log(ctx->drctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_RTT3_bd(KVReqCtx * ctx){
    kv_degread_read_metadata_bd(ctx);
    if(ctx->is_finished == true){
        return;
    }
    kv_degread_read_PL_index_bd(ctx);
    kv_degread_cas_stripe_meta_bd(ctx);
    if(ctx->drctx->is_finished == true){
        ctx->is_finished = true;
        ctx->ret_val.value_addr = ctx->drctx->ret_val.value_addr;
        return;
    }
    kv_degread_write_stripe_log_bd(ctx);
    kv_degread_RTT3_check(ctx);
}

void Client::kv_degread_analysis_metadata(KVReqCtx * ctx){
    bitset<8UL> *bitmap;
    bitmap = new bitset<8UL>[BIT_MAP_SIZE];
    uint64_t pre_kv_meta_ptr = ctx->drctx->pre_kv_meta;
    u8 *pre_kv_meta;
    int kv_off = ctx->drctx->kv_meta_ctx->off;
    int position = kv_off / 8;
    int offset = kv_off % 8;
    int num_subblock = 1;
    pre_kv_meta = (u8 *)pre_kv_meta_ptr;
    for(int i = 0;i < BIT_MAP_SIZE;i ++){
        bitmap[i] = pre_kv_meta[i];
    }

    // find num subblock of the degrade read kv
    {
        for(int i = position;i < BIT_MAP_SIZE;i ++){
            int j;
            if(i == position){
                j = offset + 1;
            }
            else {
                j = 0;
            }

            for(;j < 8;j ++){
                if(bitmap[i][j] == 0){
                    num_subblock ++;
                }
                else {
                    break;
                }
            }
            if(bitmap[i][j] == 1){
                break;
            }
        }
    }

    ctx->drctx->num_block = num_subblock;
    pre_kv_meta_ptr = (uint64_t)ctx->drctx->ec_meta_buf_;
    int index = 0;
    int first_position, first_offset;
    int kv_end_off = kv_off + num_subblock;
    int end_position, end_offset;
    end_position = kv_end_off / 8;
    end_offset = kv_end_off % 8;
    for(int t = 0;t < k;t ++){
        if(t == ctx->drctx->kv_meta_ctx->cid){
            continue;
        }
        pre_kv_meta = (u8*) ctx->drctx->kv_read_metadata_addr_list[t].front().l_kv_addr;
        ctx->drctx->need_kv[index].first_off = 0;
        ctx->drctx->need_kv[index].first_len = 1;
        ctx->drctx->need_kv[index].num_off = 0;
        ctx->drctx->need_kv[index].end_off = 1;
        ctx->drctx->need_kv[index].num_kv = 0;
        ctx->drctx->need_kv[index].metadata_addr = (uint64_t)pre_kv_meta + BIT_MAP_SIZE + 2;

        for(int i = 0;i < BIT_MAP_SIZE;i ++){
            bitmap[i] = pre_kv_meta[i];
        }

        // compute the first off
        for(int i = position;i >= 0;i --){
            int j;
            if(i == position){
                j = offset;
            }
            else{
                j = 7;
            }
            for(;j >= 0;j --){
                if(bitmap[i][j] == 0){
                    ctx->drctx->need_kv[index].first_off ++;
                }
                else{
                    break;
                }
            }
            if(bitmap[i][j] != 0){
                first_position = i;
                first_offset = j;
                break;
            }
        }

        // compute the first kv len
        for(int i = first_position;i < BIT_MAP_SIZE;i ++){
            int j;
            if(i == first_position){
                j = first_offset;
            }
            else {
                j = 0;
            }
            for(;j < 8;j ++){
                if(i == first_position && j == first_offset){
                    continue;
                }
                if(bitmap[i][j] == 0){
                    ctx->drctx->need_kv[index].first_len++;
                }
                else {
                    break;
                }
            }
            if(bitmap[i][j] == 1){
                break;
            }
        }

        // compute the num off
        for(int i = 0;i <= first_position;i ++){
            if(i != first_position){
                ctx->drctx->need_kv[index].num_off += bitmap[i].count();
            }
            else{
                for(int j = 0;j < first_offset;j ++){
                    if(bitmap[i][j] == 1){
                        ctx->drctx->need_kv[index].num_off++;
                    }
                }
            }
        }

        // compute the end off
        for(int i = end_position;i >= 0;i --){
            int j;
            if(i == end_position){
                j = end_offset;
            }
            else {
                j = 7;
            }
            for(;j >= 0;j --){
                if(bitmap[i][j] == 0){
                    ctx->drctx->need_kv[index].end_off++;
                }
                else {
                    break;
                }
            }
            if(bitmap[i][j] == 1){
                break;
            }
        }

        // compute the need kv;
        int position_1 = (kv_off + 1) / 8;
        int offset_1 = (kv_off + 1) % 8;
        int j_end;
        for(int i = position_1;i <= end_position;i ++){
            int j;
            if(i == end_position){
                j = offset_1;
            }
            else {
                j = 0;
            }
            j_end = (i == end_position ? end_offset : 8);
            for(;j < j_end;j ++){
                if(bitmap[i][j] == 1){
                    ctx->drctx->need_kv[index].num_kv ++;
                }
            }
        }
        ctx->drctx->need_kv[index].num_kv ++;
        index ++;
    }
}

void Client::kv_degread_prepare_get_kv_bucket_hash_info(KVReqCtx * ctx){
    int index = 0;
    int kv_index = 0;
    for(int t = 0;t < k;t ++){
        if(t == ctx->drctx->kv_meta_ctx->cid){
            continue;
        }
        int first_off = ctx->drctx->need_kv[index].num_off;
        int num_kv = ctx->drctx->need_kv[index].num_kv; 
        EcMeta* ec_meta_list = (EcMeta *)ctx->drctx->need_kv[index].metadata_addr;
        uint64_t key_addr;
        int key_len;
        for(int i = first_off;i < first_off + num_kv;i ++){
            key_addr = (uint64_t)ec_meta_list[i].key;
            key_len = ec_meta_list[i].key_len;
            ctx->drctx->kv_hash_info[kv_index].hash_value = 
                VariableLengthHash((void *)key_addr, key_len, 0);
            hash_compute_fp(&ctx->drctx->kv_hash_info[kv_index]);
            kv_index ++;
        }
        index ++;
    }
    ctx->drctx->num_need_kv = kv_index;
}

void Client::kv_degread_prepare_get_kv_bucket_addr_info(KVReqCtx * ctx){
    for(int i = 0;i < ctx->drctx->num_need_kv;i ++){
        get_server_subtable_kv(&ctx->drctx->kv_hash_info[i], &ctx->drctx->kv_addr_info[i]);
    }
}

void Client::kv_degread_prepare_get_kv_bucket(KVReqCtx * ctx){
    kv_degread_prepare_get_kv_bucket_hash_info(ctx);
    kv_degread_prepare_get_kv_bucket_addr_info(ctx);
}

void Client::kv_degread_get_kv_bucket(KVReqCtx * ctx){
    kv_degread_analysis_metadata(ctx);
    kv_degread_prepare_get_kv_bucket(ctx);
    uint32_t read_bucket_sr_list_num;
    IbvSrList * sr_list;
    for(int i = 0;i < ctx->drctx->num_need_kv;i ++){
        sr_list = gen_one_sr_list((uint64_t)ctx->drctx->kv_bucket_buffer[i].local_bucket_addr, KV_BUCKET_SIZE * 2, 
            local_buf_mr_->lkey, ctx->coro_id, ctx->drctx->kv_addr_info[i].server_id_list[0], 
            READ_KV_BUCKET, i, IBV_WR_RDMA_READ, ctx->drctx->kv_addr_info[i].f_bucket_addr[0],
            0, NULL);
        send_one_sr_list(sr_list, ctx->drctx->comp_wrid_map);
    }
}

void Client::kv_degread_analysis_P0_bucket(KVReqCtx * ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    get_bucket_info_big(ctx->drctx->P0_bucket_buffer);
    ctx->drctx->kv_P0_idx_list.clear();
    ctx->drctx->kv_read_P0_addr_list.clear();
    bool if_find;
    uint64_t local_P0_addr = (uint64_t)ctx->drctx->P0_bucket_buffer->local_kv_addr;
    if_find = find_P0_slot((void *)drctx->P0_bucket_buffer->local_bucket_addr, drctx->P0_hash_info->fp,
        &local_P0_addr, ctx->drctx->kv_read_P0_addr_list, ctx->drctx->kv_P0_idx_list);
    if(if_find == false){
        RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " 
            << "degrade read workflow:analysis_P0_bucket failed!";
        ctx->is_finished = true;
        ctx->ret_val.value_addr = NULL;
        exit(0);
        return;
    } else if(ctx->drctx->kv_read_P0_addr_list.size() > 1){
        RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " 
            << "degrade read workflow:analysis_P0_bucket match more!";
        ctx->is_finished = true;
        ctx->ret_val.value_addr = NULL;
        return;
    }
}

void Client::kv_degread_get_P0(KVReqCtx * ctx){
    kv_degread_analysis_P0_bucket(ctx);
    if(ctx->is_finished == true){
        return;
    }
    uint32_t read_P0_sr_list_num;
    IbvSrList * read_P0_sr_list;
    read_P0_sr_list = gen_read_value_sr_lists(ctx->coro_id, READ_P0,
        ctx->drctx->kv_read_P0_addr_list[0]);
    send_one_sr_list(read_P0_sr_list, ctx->drctx->comp_wrid_map);
}

int Client::kv_degrade_read_analysis_logging_bucket(KVReqCtx * ctx){
    get_bucket_info(ctx->drctx->PL_bucket_buffer);
    ctx->drctx->kv_read_logging_addr_list.clear();
    int if_find;
    uint64_t local_logging_addr = (uint64_t)ctx->drctx->
        PL_bucket_buffer->local_kv_addr;
    if_find = race_find_bucket_no_idx(ctx->drctx->PL_bucket_buffer->slot_arr,
        ctx->drctx->PL_hash_info, &local_logging_addr, local_buf_mr_->lkey,
        ctx->drctx->kv_read_logging_addr_list);
    if(if_find == 0){
        // RDMA_LOG_IF(2, if_print_log) << "server:" << my_server_id_ << " " << "degrade read workflow:analysis_logging_bucket there is no logging!";
        return -1;
    } 
    return 0;
}

void Client::kv_degread_get_PL(KVReqCtx * ctx){
    int ret;
    ret = kv_degrade_read_analysis_logging_bucket(ctx);
    if(ret == -1){
        return;
    }
    uint32_t read_logging_sr_list_num;
    IbvSrList * read_logging_sr_list;
    read_logging_sr_list = gen_read_kv_sr_lists_wr_add(ctx->coro_id, ctx->drctx->
        kv_read_logging_addr_list, &read_logging_sr_list_num, READ_LOGGING);
    send_one_sr_list(read_logging_sr_list, ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_RTT4_sync(KVReqCtx * ctx){
    int ret;
    kv_degread_init_sr_list(ctx);
    kv_degread_get_P0(ctx);
    kv_degread_get_PL(ctx);
    kv_degread_get_kv_bucket(ctx);
    ret = nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
    if(ret == -1){
        print_error("degrade read workflow:RTT4 send sr list failed~");
        exit(0);
    }
}

void Client::kv_degread_RTT4_async(KVReqCtx * ctx){
    kv_degread_init_sr_list(ctx);
    kv_degread_get_P0(ctx);
    kv_degread_get_PL(ctx);
    kv_degread_get_kv_bucket(ctx);
    poll_completion(*ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_read_P0_bd(KVReqCtx *ctx){
    kv_degread_get_P0(ctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_read_PL_bd(KVReqCtx *ctx){
    kv_degread_get_PL(ctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_read_related_kv_index_bd(KVReqCtx *ctx){
    kv_degread_get_kv_bucket(ctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_RTT4_bd(KVReqCtx * ctx){
    kv_degread_read_P0_bd(ctx);
    kv_degread_read_PL_bd(ctx);
    kv_degread_read_related_kv_index_bd(ctx);
}

void Client::kv_degread_read_analysis_kv_bucket(KVReqCtx * ctx){
    DegradeReadCtx *drctx = ctx->drctx;
    ctx->drctx->kv_read_kv_addr_list.clear();
    int if_find;
    uint64_t local_kv_addr;
    local_kv_addr = (uint64_t)ctx->drctx->kv_bucket_buffer[0].local_kv_addr;
    ctx->is_finished = false;
    std::pair<int32_t, int32_t> idx_list;
    for(int i = 0;i < ctx->drctx->num_need_kv;i ++){
        get_bucket_info(&ctx->drctx->kv_bucket_buffer[i]);
        KVRWAddr rw;
        if_find = find_kv_slot_in_buckets((KvEcMetaBucket *)drctx->kv_bucket_buffer[i].local_bucket_addr, &rw, 
            &idx_list, drctx->kv_hash_info[i].fp, local_kv_addr, KV_REQ_SEARCH);
        drctx->kv_read_kv_addr_list.push_back(rw);
        local_kv_addr += rw.length;
        if(if_find == 0){
            // RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " 
            //     << "degrade read workflow:analysis_kv_bucket:find [" 
            //     << i << " ] kv failed!";
            ctx->is_finished = true;
            ctx->ret_val.value_addr = NULL;
            return;
        } else if(if_find > 1){
            // RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " 
            //     << "degrade read workflow:analysis_kv_bucket:find [" 
            //     << i << " ] kv match more!";
            ctx->is_finished = true;
            ctx->ret_val.value_addr = NULL;
            return;
        }
    }
}

void Client::kv_degread_prepare_get_kv(KVReqCtx * ctx){
    kv_degread_read_analysis_kv_bucket(ctx);
    if(ctx->is_finished == true){
        return;
    }
    IbvSrList * read_kv_sr_list;
    for(int i = 0;i < ctx->drctx->num_need_kv;i ++){
        read_kv_sr_list = gen_read_value_sr_lists(ctx->coro_id, READ_KV + i,
            ctx->drctx->kv_read_kv_addr_list[i]);
        send_one_sr_list(read_kv_sr_list, ctx->drctx->comp_wrid_map);
    }
}

void Client::kv_degread_RTT5_sync(KVReqCtx * ctx){
    int ret;
    kv_degread_init_sr_list(ctx);
    kv_degread_prepare_get_kv(ctx);
    ret = nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
    if(ret == -1){
        print_error("degrade read workflow:RTT5 send sr list failed~");
        exit(0);
    }
}

void Client::kv_degread_RTT5_async(KVReqCtx * ctx){
    kv_degread_init_sr_list(ctx);
    kv_degread_prepare_get_kv(ctx);
    poll_completion(*ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_read_related_kv_bd(KVReqCtx *ctx){
    kv_degread_prepare_get_kv(ctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_RTT5_bd(KVReqCtx * ctx){
    kv_degread_read_related_kv_bd(ctx);
}

void Client::kv_degread_read_merge_logging(KVReqCtx * ctx){
    int num_logging = ctx->drctx->kv_read_logging_addr_list.size();
    u8 *parity = (u8 *)ctx->drctx->P0_bucket_buffer->local_kv_addr;
    ParityLogging * parity_logging;
    u8 *logging_old_value;
    for(int i = 0;i < num_logging;i ++){
        parity_logging = (ParityLogging *)ctx->drctx->kv_read_logging_addr_list[i].l_kv_addr;
        logging_old_value = (u8 *)((uint64_t)parity_logging + sizeof(ParityLogging));
        ec_encode_data(parity_logging->len, 1, 1, ectx->rs_coding->g_tbls_encoding, 
            &logging_old_value, &logging_old_value);
        for(int j = 0;j < parity_logging->len;j ++){
            parity[parity_logging->off * mm_->subblock_sz_ + j] ^= logging_old_value[j];
        }
    }
}

void Client::kv_degread_read_decoding(KVReqCtx * ctx){
    int index = 0;
    u8 **data = ctx->drctx->encoding_data;
    uint64_t local_kv_addr;
    int cpy_len;
    for(int i = 0;i < k - 1;i ++){
        local_kv_addr = ctx->drctx->kv_read_kv_addr_list[index].l_kv_addr;
        data[i] = (u8 *)(local_kv_addr + ctx->drctx->
            need_kv[i].first_off * mm_->subblock_sz_);
        index += ctx->drctx->need_kv[i].num_kv;
    }
    uint64_t cpy_addr = (uint64_t)ctx->drctx->P0_bucket_buffer->local_kv_addr + 
        ctx->drctx->kv_meta_ctx->off * mm_->subblock_sz_;
    cpy_len = ctx->drctx->num_block * mm_->subblock_sz_;
    data[k - 1] = (u8*)cpy_addr;
    u8 frag_error[1];
    frag_error[0] = ctx->drctx->kv_meta_ctx->cid;
    ectx->rs_coding->decode_data(data, cpy_len, frag_error, 1);
    KvValueLen *value_len_addr = (KvValueLen *)((uint64_t)data[k] + sizeof(KvKeyLen));
    int key_len = char_to_int((u8 *)data[k], sizeof(KvKeyLen));
    ctx->ret_val.value_addr = (void *)((uint64_t)data[k] + 
        sizeof(KvKeyLen) + sizeof(KvValueLen) + key_len);
}

void Client::kv_degread_read_compute_kv(KVReqCtx * ctx){
    kv_degread_read_merge_logging(ctx);
    kv_degread_read_decoding(ctx);
}

void Client::kv_degread_cas_stripe_meta_second(DegradeReadCtx *drctx){
    kv_degread_cas_stripe_meta(drctx, false);
}

void Client::kv_degread_last_RTT_log(DegradeReadCtx *drctx){
    make_log_entry((LogEntry *)drctx->stripe_meta_end_log_buf_addr,
        LOG_DEGREAD_END, drctx->stripe_id);
    IbvSrList * sr_list;
    uint8_t server_id = drctx->stripe_meta_addr_info->server_id_list[0];
    sr_list = gen_sr_list(drctx->stripe_meta_end_log_buf_addr,
        LOG_ENTRY_LEN, local_buf_mr_->lkey, drctx->coro_id, server_id, DEGREAD_LOG_END,
        0, IBV_WR_RDMA_WRITE, get_log_entry_addr(server_id));
}

void Client::kv_degread_RTT6_sync(KVReqCtx * ctx){
    int ret;
    kv_degread_init_sr_list(ctx);
    kv_degread_cas_stripe_meta_second(ctx->drctx);
    if(ctx->is_finished != true){
        // kv_degread_last_RTT_log(ctx->drctx);
        kv_degread_read_compute_kv(ctx);
    }
    ret = nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
    if(ret == -1){
        RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " << "degrade read workflow:read_faa_stripe_meta send sr list failed!";
    }
}

void Client::kv_degread_RTT6_async(KVReqCtx * ctx){
    kv_degread_init_sr_list(ctx);
    kv_degread_cas_stripe_meta_second(ctx->drctx);
    if(ctx->is_finished != true){
        kv_degread_last_RTT_log(ctx->drctx);
        kv_degread_read_compute_kv(ctx);
    }
    poll_completion(*ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_faa_stripe_meta_bd(KVReqCtx *ctx){
    kv_degread_cas_stripe_meta(ctx->drctx, false);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_decoding_bd(KVReqCtx *ctx){
    kv_degread_read_compute_kv(ctx);
}

void Client::kv_degread_write_end_log_bd(KVReqCtx *ctx){
    kv_degread_last_RTT_log(ctx->drctx);
    nm_->rdma_post_check_sr_list(ctx->drctx->comp_wrid_map);
}

void Client::kv_degread_RTT6_bd(KVReqCtx * ctx){
    kv_degread_faa_stripe_meta_bd(ctx);
    kv_degread_write_end_log_bd(ctx);
    kv_degread_decoding_bd(ctx);
}

void *Client::kv_degread_sync(KVReqCtx * ctx){
    kv_degread_RTT1_sync(ctx);
    kv_degread_RTT2_sync(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.value_addr;
    }
    kv_degread_RTT3_sync(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.value_addr;
    }
    kv_degread_RTT4_sync(ctx);
    kv_degread_RTT5_sync(ctx); 
    kv_degread_RTT6_sync(ctx);
    return ctx->ret_val.value_addr;
}

void *Client::kv_degread_bd(KVReqCtx * ctx){
    kv_degread_RTT1_bd(ctx);
    kv_degread_RTT2_bd(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.value_addr;
    }
    kv_degread_RTT3_bd(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.value_addr;
    }
    kv_degread_RTT4_bd(ctx);
    kv_degread_RTT5_bd(ctx); 
    kv_degread_RTT6_bd(ctx);
    return ctx->ret_val.value_addr;
}

void *Client::kv_degrade_read(KVReqCtx * ctx){
    kv_degread_RTT1_async(ctx);
    kv_degread_RTT2_async(ctx);
    if(ctx->is_finished == true){
        poll_completion(*ctx->drctx->comp_wrid_map);
        return ctx->ret_val.value_addr;
    }
    kv_degread_RTT3_async(ctx);
    if(ctx->is_finished == true){
        poll_completion(*ctx->drctx->comp_wrid_map);
        return ctx->ret_val.value_addr;
    }
    kv_degread_RTT4_async(ctx);
    kv_degread_RTT5_async(ctx); 
    kv_degread_RTT6_async(ctx);
    return ctx->ret_val.value_addr;
}

IbvSrList * Client::gen_write_kv_sr_lists(uint32_t coro_id, KVInfo * a_kv_info, ClientMMAllocCtx * r_mm_info,
        __OUT uint32_t * num_sr_lists) {
    IbvSrList * ret_sr_list = get_one_sr_list();
    struct ibv_send_wr * sr = get_one_sr();
    struct ibv_sge     * sge = get_one_sge();
    uint8_t server_id = r_mm_info->server_id_list;
    sge->addr   = (uint64_t)a_kv_info->l_addr;
    sge->length = r_mm_info->num_subblocks * mm_->subblock_sz_;
    sge->lkey   = a_kv_info->lkey;
    sr->wr_id   = ib_gen_wr_id(coro_id, r_mm_info->server_id_list, 
        WRITE_KV_ST_WRID, 1);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode  = IBV_WR_RDMA_WRITE;
    sr->wr.rdma.remote_addr = r_mm_info->addr_list;
    sr->wr.rdma.rkey        = server_mr_info_map_[server_id]->rkey;
    sr->next    = NULL;
    ret_sr_list->sr_list   = sr;
    ret_sr_list->num_sr    = 1;
    ret_sr_list->server_id = server_id;
    *num_sr_lists = 1;
    return ret_sr_list;
}

IbvSrList * Client::gen_write_kv_sr_lists(uint32_t coro_id, KVInfo * a_kv_info, 
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

IbvSrList * Client::gen_write_kv_sr_lists_encoding(uint32_t coro_id, KVInfo * a_kv_info, 
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

IbvSrList * Client::gen_cas_sr_lists(uint32_t coro_id, 
    const std::vector<KVCASAddr> & cas_addr_list, 
    __OUT uint32_t * num_sr_lists) {
    IbvSrList * ret_sr_list = get_one_sr_list();
    struct ibv_send_wr * sr = get_one_sr();
    struct ibv_sge     * sge = get_one_sge();
    sge->addr = cas_addr_list[0].l_kv_addr;
    sge->length = 8;
    sge->lkey = cas_addr_list[0].lkey;
    sr->wr_id = ib_gen_wr_id(coro_id, cas_addr_list[0].server_id, CAS_ST_WRID, 1);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr->wr.atomic.remote_addr = cas_addr_list[0].r_kv_addr;
    sr->wr.atomic.rkey        = cas_addr_list[0].rkey;
    sr->wr.atomic.compare_add = cas_addr_list[0].orig_value;
    sr->wr.atomic.swap        = cas_addr_list[0].swap_value;
    sr->send_flags = 0;
    sr->next = NULL;
    ret_sr_list->sr_list = sr;
    ret_sr_list->num_sr  = 1;
    ret_sr_list->server_id = cas_addr_list[0].server_id;   
    *num_sr_lists = 1;
    return ret_sr_list;
}

IbvSrList * Client::gen_cas_sr_lists_add_id(uint32_t coro_id, 
    const std::vector<KVCASAddr> & cas_addr_list, 
    __OUT uint32_t * num_sr_lists, uint64_t add_id) {
    IbvSrList * ret_sr_list = get_one_sr_list();
    struct ibv_send_wr * sr = get_one_sr();
    struct ibv_sge     * sge = get_one_sge();
    memset(sr, 0, sizeof(struct ibv_send_wr));
    memset(sge, 0, sizeof(struct ibv_sge));
    sge->addr = cas_addr_list[0].l_kv_addr;
    sge->length = 8;
    sge->lkey = cas_addr_list[0].lkey;
    sr->wr_id = ib_gen_wr_id(coro_id, cas_addr_list[0].server_id, add_id, 1);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr->wr.atomic.remote_addr = cas_addr_list[0].r_kv_addr;
    sr->wr.atomic.rkey        = cas_addr_list[0].rkey;
    sr->wr.atomic.compare_add = cas_addr_list[0].orig_value;
    sr->wr.atomic.swap        = cas_addr_list[0].swap_value;
    sr->send_flags = 0;
    sr->next = NULL;
    ret_sr_list->sr_list = sr;
    ret_sr_list->num_sr  = 1;
    ret_sr_list->server_id = cas_addr_list[0].server_id;
    *num_sr_lists = 1;
    return ret_sr_list;
}

IbvSrList * Client::gen_cas_sr_lists_encoding(uint32_t coro_id, 
    const std::vector<KVCASAddr> & cas_addr_list, 
    __OUT uint32_t * num_sr_lists, uint64_t add_id) {
    IbvSrList * ret_sr_list = get_one_sr_list_encoding();
    struct ibv_send_wr * sr = get_one_sr_encoding();
    struct ibv_sge     * sge = get_one_sge_encoding();
    memset(sr, 0, sizeof(struct ibv_send_wr));
    memset(sge, 0, sizeof(struct ibv_sge));
    sge->addr = cas_addr_list[0].l_kv_addr;
    sge->length = 8;
    sge->lkey = cas_addr_list[0].lkey;
    sr->wr_id = ib_gen_wr_id(coro_id, cas_addr_list[0].server_id, add_id, 0);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr->wr.atomic.remote_addr = cas_addr_list[0].r_kv_addr;
    sr->wr.atomic.rkey        = cas_addr_list[0].rkey;
    sr->wr.atomic.compare_add = cas_addr_list[0].orig_value;
    sr->wr.atomic.swap        = cas_addr_list[0].swap_value;
    sr->next = NULL;
    ret_sr_list->sr_list = sr;
    ret_sr_list->num_sr  = 1;
    ret_sr_list->server_id = cas_addr_list[0].server_id; 
    *num_sr_lists = 1;
    return ret_sr_list;
}

IbvSrList * Client::gen_faa_sr_lists(uint32_t coro_id, 
    const std::vector<KVCASAddr> & cas_addr_list,
    __OUT uint32_t * num_sr_lists, uint64_t faa_value){
    IbvSrList * ret_sr_lists = get_one_sr_list();
    struct ibv_send_wr * sr  = get_one_sr();
    struct ibv_sge     * sge = get_one_sge();
    sge->addr = cas_addr_list[0].l_kv_addr;
    sge->length = 8;
    sge->lkey = cas_addr_list[0].lkey;
    sr->wr_id = ib_gen_wr_id(coro_id, cas_addr_list[0].server_id, FAA_ST_WRID, 1);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    sr->wr.atomic.remote_addr = cas_addr_list[0].r_kv_addr;
    sr->wr.atomic.rkey        = cas_addr_list[0].rkey;
    sr->wr.atomic.compare_add = faa_value;
    sr->send_flags = 0;
    sr->next = NULL;
    ret_sr_lists->sr_list = sr;
    ret_sr_lists->num_sr  = 1;
    ret_sr_lists->server_id = cas_addr_list[0].server_id;
    *num_sr_lists = 1;
    return ret_sr_lists;
}

IbvSrList * Client::gen_read_kv_sr_lists(uint32_t coro_id, const std::vector<KVRWAddr> & r_addr_list, 
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

IbvSrList * Client::gen_read_value_sr_lists(uint32_t coro_id, int wr_add, KVRWAddr r_addr_list){
    IbvSrList * ret_sr_list = get_one_sr_list();
    struct ibv_send_wr * sr = get_one_sr();
    struct ibv_sge     * sge = get_one_sge();
    sge[0].addr   = r_addr_list.l_kv_addr;
    sge[0].length = r_addr_list.length;
    sge[0].lkey   = r_addr_list.lkey;
    sr[0].wr_id =  ib_gen_wr_id(coro_id, r_addr_list.server_id, wr_add, 1);
    sr[0].sg_list = &sge[0];
    sr[0].num_sge = 1;
    sr[0].opcode  = IBV_WR_RDMA_READ;
    sr[0].wr.rdma.remote_addr = r_addr_list.r_kv_addr;
    sr[0].wr.rdma.rkey = r_addr_list.rkey;
    ret_sr_list[0].sr_list   = sr;
    ret_sr_list[0].server_id = r_addr_list.server_id;
    ret_sr_list[0].num_sr    = 1;
    return ret_sr_list;
}

IbvSrList * Client::gen_read_kv_sr_lists_wr_add(uint32_t coro_id, 
    const std::vector<KVRWAddr> & r_addr_list, __OUT uint32_t * num_sr_lists, int wr_add) {
    std::map<uint8_t, std::vector<KVRWAddr> > server_id_kv_addr_map;
    for (size_t i = 0; i < r_addr_list.size(); i ++) {
        server_id_kv_addr_map[r_addr_list[i].server_id].push_back(r_addr_list[i]);
    }
    IbvSrList * ret_sr_list = (IbvSrList *)malloc(sizeof(IbvSrList) * server_id_kv_addr_map.size());
    std::map<uint8_t, std::vector<KVRWAddr> >::iterator it;
    uint32_t sr_num_cnt = 0;
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
            sr[i].wr_id =  ib_gen_wr_id(coro_id, it->first, wr_add, i + 1);
            sr[i].sg_list = &sge[i];
            sr[i].num_sge = 1;
            sr[i].opcode  = IBV_WR_RDMA_READ;
            sr[i].wr.rdma.remote_addr = it->second[i].r_kv_addr;
            sr[i].wr.rdma.rkey = it->second[i].rkey;
            if (i != it->second.size() - 1) {
                sr[i].next = &sr[i + 1];
            }
        }
        ret_sr_list[sr_num_cnt].sr_list   = sr;
        ret_sr_list[sr_num_cnt].server_id = it->first;
        ret_sr_list[sr_num_cnt].num_sr    = it->second.size();
        sr_num_cnt ++;
    }
    *num_sr_lists = server_id_kv_addr_map.size();
    return ret_sr_list;
}

IbvSrList *Client::gen_insert_write_kv_log(KVReqCtx * ctx){
    uint32_t kv_block_size = ctx->kv_all_len;
    uint64_t server_id = hashring->hash_map_id(ctx->hash_info.hash_value, 1);
    mm_->mm_alloc(kv_block_size, nm_, server_id, &ctx->insert_kv_log_mm_alloc_ctx);
    IbvSrList * ret_sr_list = get_one_sr_list();
    struct ibv_send_wr * sr = get_one_sr();
    struct ibv_sge     * sge = get_one_sge();
    memset(sr, 0, sizeof(ibv_send_wr));
    memset(sge, 0, sizeof(ibv_sge));
    sge->addr   = (uint64_t)ctx->kv_info->l_addr;
    sge->length = ctx->insert_kv_log_mm_alloc_ctx.num_subblocks * mm_->subblock_sz_;
    sge->lkey   = ctx->kv_info->lkey;
    sr->wr_id   = ib_gen_wr_id(ctx->coro_id, server_id, INSERT_WRITE_KV_LOG, 1);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode  = IBV_WR_RDMA_WRITE;
    sr->wr.rdma.remote_addr = ctx->insert_kv_log_mm_alloc_ctx.addr_list;
    sr->wr.rdma.rkey        = ctx->insert_kv_log_mm_alloc_ctx.rkey_list;
    sr->next    = NULL;
    ret_sr_list->sr_list   = sr;
    ret_sr_list->num_sr    = 1;
    ret_sr_list->server_id = server_id;
    return ret_sr_list;
}

IbvSrList *Client::gen_insert_write_kv_log_entry(KVReqCtx * ctx){
    IbvSrList * ret_sr_list = get_one_sr_list();
    struct ibv_send_wr * sr = get_one_sr();
    struct ibv_sge     * sge = get_one_sge();
    uint64_t server_id = ctx->insert_kv_log_mm_alloc_ctx.server_id_list;
    RaceHashSlot slot;
    fill_slot(&ctx->insert_kv_log_mm_alloc_ctx, &ctx->hash_info, &slot);
    make_log_entry(ctx->insert_kv_log_buf_, LOG_INSERT_CAS_KV_SLOT,
        *(uint64_t *)&slot);
    sge->addr   = (uint64_t)(void *)ctx->insert_kv_log_buf_;
    sge->length = LOG_ENTRY_LEN;
    sge->lkey   = local_buf_mr_->lkey;
    sr->wr_id   = ib_gen_wr_id(ctx->coro_id, server_id, INSERT_WRITE_KV_LOG_ENTRY, 1);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode  = IBV_WR_RDMA_WRITE;
    sr->wr.rdma.remote_addr = get_log_entry_addr(server_id);
    sr->wr.rdma.rkey        = server_mr_info_map_[server_id]->rkey;
    sr->next    = NULL;
    ret_sr_list->sr_list   = sr;
    ret_sr_list->num_sr    = 1;
    ret_sr_list->server_id = server_id;
    return ret_sr_list;
}   

IbvSrList *Client::gen_faa_log(int coro_id, uint8_t server_id, bool if_wp, 
    uint64_t faa_value, uint64_t return_addr, int lkey, int wr_id_off){
    IbvSrList * ret_sr_list = get_one_sr_list();
    struct ibv_send_wr * sr = get_one_sr();
    struct ibv_sge     * sge = get_one_sge();
    sge->addr   = return_addr;
    sge->length = sizeof(uint64_t);
    sge->lkey   = lkey;
    sr->wr_id = ib_gen_wr_id(coro_id, server_id, INSERT_WRITE_FAA_WP_LOG, wr_id_off);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode  = IBV_WR_ATOMIC_FETCH_AND_ADD;
    sr->wr.atomic.remote_addr = if_wp ? client_log->log_list[server_id].remote_wp_addr : 
        client_log->log_list[server_id].remote_rp_addr;
    sr->wr.atomic.rkey        = server_mr_info_map_[server_id]->rkey;
    sr->wr.atomic.compare_add = faa_value;
    sr->send_flags = 0;
    sr->next = NULL;
    ret_sr_list->sr_list   = sr;
    ret_sr_list->num_sr    = 1;
    ret_sr_list->server_id = server_id;
    return ret_sr_list;
}

IbvSrList *Client::gen_faa_log_encoding(int coro_id, uint8_t server_id, bool if_wp, 
    uint64_t faa_value, uint64_t return_addr, int lkey, int wr_id_off){
    IbvSrList * ret_sr_list = get_one_sr_list_encoding();
    struct ibv_send_wr * sr = get_one_sr_encoding();
    struct ibv_sge     * sge = get_one_sge_encoding();
    sge->addr   = return_addr;
    sge->length = sizeof(uint64_t);
    sge->lkey   = lkey;
    sr->wr_id = ib_gen_wr_id(coro_id, server_id, INSERT_WRITE_FAA_WP_LOG, wr_id_off);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode  = IBV_WR_ATOMIC_FETCH_AND_ADD;
    sr->wr.atomic.remote_addr = if_wp ? client_log->log_list[server_id].remote_wp_addr : 
        client_log->log_list[server_id].remote_rp_addr;
    sr->wr.atomic.rkey        = server_mr_info_map_[server_id]->rkey;
    sr->wr.atomic.compare_add = faa_value;
    sr->next = NULL;
    ret_sr_list->sr_list   = sr;
    ret_sr_list->num_sr    = 1;
    ret_sr_list->server_id = server_id;
    return ret_sr_list;
}

void Client::kv_insert_read_buckets_and_write_kv_sync(KVReqCtx * ctx) {
    int ret = 0;
    ctx->comp_wrid_map.clear();
    uint32_t kv_block_size = ctx->kv_all_len;
    uint64_t server_id = ctx->tbl_addr_info.server_id_list[0];
    mm_->mm_alloc(kv_block_size, nm_, server_id, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list < server_st_addr_ || 
        ctx->mm_alloc_ctx.addr_list >= server_st_addr_ + server_data_len_) {
        print_error("insert workflow:read_buckets_and_write_kv_sync:mm alloc failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &sr_list_num);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, server_id, READ_BUCKET_ST_WRID, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    // sr_list = gen_insert_write_kv_log(ctx);
    // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    // sr_list = gen_insert_write_kv_log_entry(ctx);
    // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    // sr_list = gen_faa_log(ctx->coro_id, 
    //     ctx->insert_kv_log_mm_alloc_ctx.server_id_list, true, 1, 
    //     (uint64_t)ctx->insert_faa_wp_return_addr, local_buf_mr_->lkey, 0);
    // send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("insert workflow:read_buckets_and_write_kv send sr list failed~");
        exit(0);
    }
    ctx->is_finished = false;
    return;
}

void Client::init_count_server(){
    count_server = new int[num_memory_];
    for(int i = 0;i < num_memory_;i ++){
        count_server[i] = 0;
    }
}

void Client::print_count_server(){
    cout << "print count server~" << endl;
    for(int i = 0;i < num_memory_;i ++){
        cout << "server:" << i << " count:" << count_server[i] << endl;
    }
    cout << "print count server finished~" << endl;
}

void Client::kv_insert_read_buckets_and_write_kv(KVReqCtx * ctx) {
    int ret = 0;
    ctx->comp_wrid_map.clear();
    uint32_t kv_block_size = ctx->kv_all_len;
    uint64_t server_id = ctx->tbl_addr_info.server_id_list[0];
    mm_->mm_alloc(kv_block_size, nm_, server_id, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list < server_st_addr_ || 
        ctx->mm_alloc_ctx.addr_list >= server_st_addr_ + server_data_len_) {
        print_error("insert workflow:read_buckets_and_write_kv_sync:mm alloc failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &sr_list_num);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, server_id, READ_BUCKET_ST_WRID, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    sr_list = gen_insert_write_kv_log(ctx);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    sr_list = gen_insert_write_kv_log_entry(ctx);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    sr_list = gen_faa_log(ctx->coro_id, 
        ctx->insert_kv_log_mm_alloc_ctx.server_id_list, true, 1, 
        (uint64_t)ctx->insert_faa_wp_return_addr, local_buf_mr_->lkey, 0);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("insert workflow:RTT1 send sr list failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }    
    ctx->is_finished = false;
    return;
}

void Client::kv_insert_write_buckets_sync_crash(KVReqCtx * ctx) {
    int ret = 0;
    find_empty_kv_slot_crash(ctx); 
    if (ctx->bucket_idx == -1) {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    ctx->comp_wrid_map.clear();
    RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
    fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
    uint64_t sub = KV_BUCKET_SIZE * ctx->bucket_idx + sizeof(KvEcMetaSlot) * ctx->slot_idx + sizeof(uint64_t) * 2;
    uint64_t remote_slot_addr;
    remote_slot_addr = ctx->tbl_addr_info.f_bucket_addr[0] + sub; 
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_cas_sr_list((uint64_t)ctx->local_cas_return_value_addr, local_buf_mr_->lkey, ctx->coro_id, 
        ctx->tbl_addr_info.server_id_list[0], CAS_ST_WRID, 0, remote_slot_addr, 0, *(uint64_t *)new_local_slot_ptr,
        IBV_SEND_SIGNALED, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("insert workflow:write_buckets send sr list failed~");
    }
    ctx->is_finished = true;
    ctx->ret_val.ret_code = 0;
    return;
}

void Client::kv_insert_write_buckets_sync(KVReqCtx * ctx) {
    int ret = 0;
    find_empty_kv_slot(ctx); 
    if (ctx->bucket_idx == -1) {
        // print_error("insert workflow:find kv buckets failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    ctx->comp_wrid_map.clear();
    RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
    fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
    uint64_t sub = KV_BUCKET_SIZE * ctx->bucket_idx + sizeof(KvEcMetaSlot) * ctx->slot_idx + sizeof(uint64_t) * 2;
    uint64_t remote_slot_addr;
    remote_slot_addr = ctx->tbl_addr_info.f_bucket_addr[0] + sub; 
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_cas_sr_list((uint64_t)ctx->local_cas_return_value_addr, local_buf_mr_->lkey, ctx->coro_id, 
        ctx->tbl_addr_info.server_id_list[0], CAS_ST_WRID, 0, remote_slot_addr, 0, *(uint64_t *)new_local_slot_ptr,
        IBV_SEND_SIGNALED, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("insert workflow:write_buckets send sr list failed~");
    }
    // check update success
    if (*(uint64_t *)ctx->local_cas_return_value_addr != 0) {
        RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " << "kv_insert_write_buckets check failed";
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        ctx->is_finished = true;
        return;
    }
    ctx->is_finished = true;
    ctx->ret_val.ret_code = 0;
    return;
}

void Client::kv_insert_write_buckets_sync_batch(KVReqCtx * ctx) {
    int ret = 0;
    find_empty_kv_slot(ctx); 
    if (ctx->bucket_idx == -1) {
        // print_error("insert workflow:find kv buckets failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    ctx->comp_wrid_map.clear();
    RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
    fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
    uint64_t sub = KV_BUCKET_SIZE * ctx->bucket_idx + sizeof(KvEcMetaSlot) * ctx->slot_idx + sizeof(uint64_t) * 2;
    uint64_t remote_slot_addr;
    remote_slot_addr = ctx->tbl_addr_info.f_bucket_addr[0] + sub; 
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    vector<IbvSrList *> sr_list_batch;
    vector<uint32_t> sr_list_num_batch;
    sr_list = gen_one_cas_sr_list((uint64_t)ctx->local_cas_return_value_addr, local_buf_mr_->lkey, ctx->coro_id, 
        ctx->tbl_addr_info.server_id_list[0], CAS_ST_WRID, 0, remote_slot_addr, 0, *(uint64_t *)new_local_slot_ptr,
        IBV_SEND_SIGNALED, NULL);
    sr_list_batch.push_back(sr_list);
    sr_list_num_batch.push_back(1);
    struct ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(sr_list_batch, sr_list_num_batch, &wc);
    if(ret == -1){
        print_error("insert workflow:write_buckets send sr list failed~");
    }
    // check update success
    if (*(uint64_t *)ctx->local_cas_return_value_addr != 0) {
        RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " << "kv_insert_write_buckets check failed";
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        ctx->is_finished = true;
        return;
    }
    ctx->is_finished = true;
    ctx->ret_val.ret_code = 0;
    return;
}

void Client::kv_insert_write_buckets(KVReqCtx * ctx) {
    int ret = 0;
    find_empty_kv_slot(ctx); 
    if (ctx->bucket_idx == -1) {
        print_error("insert workflow:find kv buckets failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    ctx->comp_wrid_map.clear();
    RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
    fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
    uint64_t sub = KV_BUCKET_SIZE * ctx->bucket_idx + sizeof(KvEcMetaSlot) * ctx->slot_idx + sizeof(uint64_t) * 2;
    uint64_t remote_slot_addr;
    remote_slot_addr = ctx->tbl_addr_info.f_bucket_addr[0] + sub; 
    uint64_t origin_value = *(uint64_t *)((uint64_t)ctx->local_bucket_addr + sub);
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_cas_sr_list((uint64_t)ctx->local_cas_return_value_addr, local_buf_mr_->lkey, ctx->coro_id, 
        ctx->tbl_addr_info.server_id_list[0], CAS_ST_WRID, 0, remote_slot_addr, origin_value, *(uint64_t *)new_local_slot_ptr,
        IBV_SEND_SIGNALED, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("insert workflow:RTT2 send sr list failed~");
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        ctx->is_finished = true;
        return;
    }
    if (*(uint64_t *)sr_list->sr_list->sg_list->addr != sr_list->sr_list->wr.atomic.compare_add) {
        // print_error("insert failed~");
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        ctx->is_finished = true;
        return;
    }
    ctx->is_finished = false;
    ctx->ret_val.ret_code = 0;
    return;
}

void Client::kv_search_read_buckets_sync(KVReqCtx * ctx) {
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

IbvSrList *Client::gen_one_sr_list_batch(uint64_t local_addr, uint32_t local_len, uint32_t lkey, 
    uint32_t coro_id, uint8_t server_id, uint32_t type, uint32_t wr_id_offset, 
    ibv_wr_opcode opcode, uint64_t remote_addr, int send_flags, ibv_send_wr *next){
    IbvSrList          * sr_list = (IbvSrList *) malloc (sizeof(IbvSrList));
    struct ibv_send_wr * sr      = (ibv_send_wr *) malloc (sizeof(ibv_send_wr));
    struct ibv_sge     * sge     = (ibv_sge *) malloc (sizeof(ibv_sge));
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

void Client::kv_search_read_buckets_sync_batch(KVReqCtx * ctx) {
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_sr_list_batch((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, ctx->tbl_addr_info.server_id_list[0], READ_BUCKET_ST_WRID, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    vector<IbvSrList *> *sr_list_batch = new vector<IbvSrList *>;
    vector<uint32_t> *sr_list_num_batch = new vector<uint32_t>;
    sr_list_batch->push_back(sr_list);
    sr_list_num_batch->push_back(1);
    struct ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(*sr_list_batch, *sr_list_num_batch, &wc);
    free_sr_list(sr_list, 1);
    delete sr_list_batch;
    delete sr_list_num_batch;
    ctx->is_finished = false;
    return;
}

void Client::kv_search_read_buckets(KVReqCtx * ctx) {
    int ret = 0;
    ctx->comp_wrid_map.clear();
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, ctx->tbl_addr_info.server_id_list[0], READ_BUCKET_ST_WRID, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("search workflow:RTT1 send sr list failed~");
        ctx->is_finished = true;
        ctx->ret_val.value_addr = NULL;
        return;
    }
    ctx->is_finished = false;
    return;
}

void Client::kv_search_read_kv_sync(KVReqCtx * ctx) {
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

void Client::kv_search_read_kv_sync_batch(KVReqCtx * ctx) {
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
    vector<IbvSrList *> *sr_list_batch = new vector<IbvSrList *>;
    vector<uint32_t> *sr_list_num_batch = new vector<uint32_t>;
    sr_list = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &sr_list_num);
    sr_list_batch->push_back(sr_list);
    sr_list_num_batch->push_back(sr_list_num);
    struct ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(*sr_list_batch, *sr_list_num_batch, &wc);
    delete sr_list_batch;
    delete sr_list_num_batch;
    free_sr_list(sr_list, 1);
    ctx->is_finished = false;
    return;
}

void Client::kv_search_read_kv(KVReqCtx * ctx) {
    int ret = 0;
    find_kv_slot(ctx);
    ctx->comp_wrid_map.clear();
    if(ctx->kv_read_addr_list.size() == 0){
        // print_error("search workflow:read kv:find kv in buckets failed~");
        code_num_count.search_find_failed ++;
        ctx->ret_val.value_addr = NULL;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &sr_list_num);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("search workflow:RTT2 send sr list failed~");
        ctx->is_finished = true;
        ctx->ret_val.value_addr = NULL;
        return;
    }
    ctx->is_finished = false;
    return;
}

void Client::kv_search_check_kv(KVReqCtx * ctx) {
    int32_t match_idx = find_match_kv_idx(ctx);
    if (match_idx != -1) { 
        uint64_t read_key_addr = ctx->kv_read_addr_list[match_idx].l_kv_addr + sizeof(KvKeyLen) + sizeof(KvValueLen);
        KvKeyLen * header = (KvKeyLen *)ctx->kv_read_addr_list[match_idx].l_kv_addr; 
        ctx->ret_val.value_addr = (void *)(read_key_addr + char_to_int((u8 *)header, sizeof(KvKeyLen)));
        ctx->is_finished = true;
        return;
    } else {
        // print_error("search workflow: no match kv~");
        code_num_count.search_match_failed ++;
        ctx->ret_val.value_addr = NULL;
        ctx->is_finished = true;
        return;
    }
}

void Client::kv_update_init_sr_list(KVReqCtx * ctx){
    if(if_batch == true){
        ctx->uctx->sr_list_batch.clear();
        ctx->uctx->sr_list_num_batch.clear();
    }
    ctx->uctx->comp_wrid_map->clear();
    if(if_req_latency){
        ctx->wr_id_map_op_id.clear();
    }
}

void Client::kv_update_RTT1_read_bucket_and_write_kv(KVReqCtx * ctx){
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_WRITE_KV);
    }
    uint32_t kv_block_size = ctx->kv_all_len;
    uint64_t server_id = ctx->tbl_addr_info.server_id_list[0];
    mm_->mm_alloc(kv_block_size, nm_, server_id, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list < server_st_addr_ || ctx->mm_alloc_ctx.addr_list >= 
        server_st_addr_ + server_data_len_) {
        print_error("update workflow:RTT1 mm alloc failed to write kv~");
        exit(0);
    }
    // prepare write kv and read bucket sr lists
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_READ_BUCKET);
    }
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, server_id, READ_KV_BUCKET, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    if(if_req_latency){
        ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_READ_BUCKET;
    }
    kv_update_send(ctx->uctx, sr_list, 1);
    sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &sr_list_num, WRITE_KV_ST_WRID);
    if(if_req_latency){
        ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_WRITE_KV;
    }
    kv_update_send(ctx->uctx, sr_list, sr_list_num);
}

void Client::kv_update_send(UpdateCtx *uctx, IbvSrList *sr_list, uint32_t sr_list_num){
    if(if_batch == true){
        uctx->sr_list_batch.push_back(sr_list);
        uctx->sr_list_num_batch.push_back(sr_list_num);
    } else {
        send_one_sr_list(sr_list, uctx->comp_wrid_map);
    }
}

void Client::kv_update_post_and_wait(KVReqCtx *ctx){
    UpdateCtx *uctx = ctx->uctx;
    if(if_batch == true){
        nm_->rdma_post_sr_list_batch_sync_send(uctx->sr_list_batch, uctx->sr_list_num_batch, *uctx->comp_wrid_map);
    }
    if(if_req_latency){
        poll_completion_bd(*uctx->comp_wrid_map, ctx->wr_id_map_op_id, ctx->req_timer);
    } else {
        poll_completion(*uctx->comp_wrid_map);
    }
}

void Client::kv_update_RTT1_sync(KVReqCtx * ctx) {
    ctx->is_finished = false;
    kv_update_init_sr_list(ctx);
    kv_update_RTT1_read_bucket_and_write_kv(ctx);
    nm_->rdma_post_check_sr_list(ctx->uctx->comp_wrid_map);
}

void Client::kv_update_RTT1_async(KVReqCtx * ctx) {
    ctx->is_finished = false;
    kv_update_init_sr_list(ctx);
    kv_update_RTT1_read_bucket_and_write_kv(ctx);
    kv_update_post_and_wait(ctx);
}

void Client::get_bucket_info(BucketBuffer * bucketbuffer){
    bucketbuffer->f_com_bucket = bucketbuffer->local_bucket_addr;
    bucketbuffer->s_com_bucket = bucketbuffer->local_bucket_addr + 2;
    bucketbuffer->bucket_arr[0] = bucketbuffer->f_com_bucket;
    bucketbuffer->bucket_arr[1] = bucketbuffer->f_com_bucket + 1;
    bucketbuffer->bucket_arr[2] = bucketbuffer->s_com_bucket;
    bucketbuffer->bucket_arr[3] = bucketbuffer->s_com_bucket + 1;
    bucketbuffer->slot_arr[0] = bucketbuffer->f_com_bucket[0].slots;
    bucketbuffer->slot_arr[1] = bucketbuffer->f_com_bucket[1].slots;
    bucketbuffer->slot_arr[2] = bucketbuffer->s_com_bucket[0].slots;
    bucketbuffer->slot_arr[3] = bucketbuffer->s_com_bucket[1].slots;
}

void Client::get_bucket_info_big(BucketBuffer * bucketbuffer){
    bucketbuffer->f_com_bucket = bucketbuffer->local_bucket_addr;
    bucketbuffer->s_com_bucket = bucketbuffer->local_bucket_addr + 4;
    bucketbuffer->bucket_arr[0] = bucketbuffer->f_com_bucket;
    bucketbuffer->bucket_arr[1] = bucketbuffer->f_com_bucket + 2;
    bucketbuffer->bucket_arr[2] = bucketbuffer->s_com_bucket;
    bucketbuffer->bucket_arr[3] = bucketbuffer->s_com_bucket + 2;
    bucketbuffer->slot_arr[0] = bucketbuffer->f_com_bucket[0].slots;
    bucketbuffer->slot_arr[1] = bucketbuffer->f_com_bucket[1].slots;
    bucketbuffer->slot_arr[2] = bucketbuffer->s_com_bucket[0].slots;
    bucketbuffer->slot_arr[3] = bucketbuffer->s_com_bucket[1].slots;
}

int Client::find_kv_meta_slot(void * bucketbuffer, KvEcMetaCtx *kv_meta_ctx, int fp){
    KvMetaBucket *bucket = (KvMetaBucket *)bucketbuffer;
    KvMetaSlot *slot;
    for(int i = 0;i < 2;i ++){
        for(int j = 0;j < RACE_HASH_ASSOC_NUM;j ++){
            slot = (KvMetaSlot *)&bucket[i].slots[j];
            if(get_kv_meta_fp(slot) == fp){
                get_kv_meta_slot(slot, kv_meta_ctx);
                return 1;
            }
        }
    }
    return 0;
}

void Client::kv_update_analysis_kv_meta(KVReqCtx * ctx){
    KvEcMetaBucket *bucket = (KvEcMetaBucket *)ctx->local_bucket_addr;
    EcMetaSlot *slot = &bucket[ctx->bucket_idx].slots[ctx->slot_idx].ec_meta_slot;
    get_ec_meta_slot(slot, &ctx->uctx->kv_meta_ctx);
    ctx->uctx->parity_id[0] = ctx->uctx->kv_meta_ctx.pid1;
    ctx->uctx->stripe_id = ctx->uctx->kv_meta_ctx.sid;
}

void Client::kv_update_prepare_stripe_meta(UpdateCtx * uctx){
    prepare_stripe_meta(uctx->stripe_id, uctx->stripe_meta_hash_info, uctx->stripe_meta_addr_info);
}

void Client::kv_update_read_kv(KVReqCtx * ctx){
    if(ctx->kv_read_addr_list.size() == 0){
        print_error("update workflow:read kv:find kv in buckets failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList * sr_list = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &sr_list_num);
    if(if_req_latency){
        ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_READ_KV;
    }
    kv_update_send(ctx->uctx, sr_list, sr_list_num);
}

void Client::send_one_sr_list(IbvSrList * sr_list, map<uint64_t, bool> *comp_wrid_map){
    uint32_t num_sr = sr_list[0].num_sr;
    sr_list[0].sr_list[num_sr - 1].send_flags |= IBV_SEND_SIGNALED;
    if(comp_wrid_map->find(sr_list[0].sr_list[num_sr - 1].wr_id) != comp_wrid_map->end()){
        RDMA_LOG_IF(2, if_print_log) << "wr id: " << 
            sr_list[0].sr_list[num_sr - 1].wr_id << " complicate~";
    }
    comp_wrid_map[0][sr_list[0].sr_list[num_sr - 1].wr_id] = false;
    struct ibv_qp * send_qp = nm_->rc_qp_list_[sr_list[0].server_id]; 
    struct ibv_send_wr * bad_wr;
    int post_ret = ibv_post_send(send_qp, sr_list[0].sr_list, &bad_wr);
    if (post_ret != 0) {
        printf("send_one_sr_list: ibv_post_send returned %d for wr_id=%llu\n",
               post_ret, (unsigned long long)sr_list[0].sr_list[num_sr - 1].wr_id);
    }
}

void Client::kv_update_get_stripe_meta(KVReqCtx * ctx){
    kv_update_analysis_kv_meta(ctx);
    if(ctx->is_finished == true){
        return;
    }
    kv_update_prepare_stripe_meta(ctx->uctx);
    uint32_t sr_list_num;
    IbvSrList * sr_list = gen_bucket_sr_lists(ctx->coro_id, 
        ctx->uctx->stripe_meta_addr_info, 
        &ctx->uctx->stripe_meta_bucket_info, 
        &sr_list_num, READ_STRIPE_META_BUCKET);
    if(if_req_latency){
        ctx->wr_id_map_op_id[sr_list->sr_list[1].wr_id] = UPDATE_READ_STRIPE_META;
    }
    kv_update_send(ctx->uctx, sr_list, sr_list_num);
}

void Client::kv_update_RTT2_sync(KVReqCtx * ctx) {
    kv_update_init_sr_list(ctx);
    find_kv_slot(ctx);
    if(ctx->is_finished == true){
        return;
    }
    kv_update_read_kv(ctx);
    kv_update_get_stripe_meta(ctx);
    nm_->rdma_post_check_sr_list(ctx->uctx->comp_wrid_map);
}

void Client::kv_update_RTT2_async(KVReqCtx * ctx) {
    kv_update_init_sr_list(ctx);
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_READ_KV);
    }
    find_kv_slot(ctx);
    if(ctx->is_finished == true){
        code_num_count.update_find_kv_failed ++;
        return;
    }
    kv_update_read_kv(ctx);
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_READ_STRIPE_META);
    }
    kv_update_get_stripe_meta(ctx);
    kv_update_post_and_wait(ctx);
}

void Client::kv_update_RTT4_kv(KVReqCtx * ctx){
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_CAS_KV_SLOT);
    }
    std::pair<int32_t, int32_t> idx_pair = ctx->kv_idx_list[ctx->uctx->PL_match_idx];
    int32_t bucket_idx = idx_pair.first;
    int32_t slot_idx   = idx_pair.second;
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
        IBV_SEND_SIGNALED, NULL);
    if(if_req_latency){
        ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_CAS_KV_SLOT;
    }
    kv_update_send(ctx->uctx, sr_list, 1);
    // CasKvMessage *mes = &ctx->uctx->cas_rec_ele->cas_kv_mes;
    // mes->new_value = new_value;
    // mes->remote_slot_addr = remote_slot_addr;
    // mes->server_id = ctx->tbl_addr_info.server_id_list[0];
    // mes->local_addr = (uint64_t)ctx->local_cas_return_value_addr;
}

void Client::kv_update_RTT4_PL(KVReqCtx * ctx){
    RaceHashSlot * new_local_slot_ptr;
    uint64_t local_com_bucket_addr; 
    uint64_t old_local_slot_addr; 
    uint64_t remote_slot_addr;
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    ctx->is_finished = false;
    int bucket_idx, slot_idx;
    KVTableAddrInfo *addr_info = ctx->uctx->PL_addr_info;
    BucketBuffer *bucket_buffer;
    for(int i = 0;i < m;i ++){
        if(if_req_latency){
            if(i == 0){
                ctx->req_timer->start_timer(UPDATE_CAS_PL_SLOT1);
            } else if(i == 1){
                ctx->req_timer->start_timer(UPDATE_CAS_PL_SLOT2);
            }
        }
        bucket_buffer = &ctx->uctx->PL_bucket_buffer[i];
        get_bucket_info(bucket_buffer);
        if(i == 0){
            find_empty_slot_from_bucket_buffer(bucket_buffer, addr_info);
            bucket_idx = bucket_buffer->bucket_idx;
            slot_idx   = bucket_buffer->slot_idx;
        
        }
        if (bucket_idx == -1) {
            RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " << "update workflow:find ("<< i << ") parity logging buckets failed";
            ctx->is_finished = true;
            ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
            return;
        }
        new_local_slot_ptr = (RaceHashSlot *)bucket_buffer->local_cas_target_value_addr;
        fill_slot(&ctx->uctx->mm_alloc_parity_logging_ctx[i], &ctx->uctx->PL_hash_info[i], new_local_slot_ptr); 
        if (bucket_idx < 2) {
            local_com_bucket_addr = (uint64_t)bucket_buffer->f_com_bucket; 
            old_local_slot_addr = (uint64_t)&(bucket_buffer->f_com_bucket[bucket_idx].slots[slot_idx]);
            remote_slot_addr = addr_info->f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
        } else {
            local_com_bucket_addr = (uint64_t)bucket_buffer->s_com_bucket; 
            old_local_slot_addr = (uint64_t)&(bucket_buffer->s_com_bucket[bucket_idx - 2].slots[slot_idx]); 
            remote_slot_addr = addr_info->s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr); 
        }
        if(i == 0){
            fill_cas_slot_through_bucket_buffer_value(&bucket_buffer->kv_modify_pr_cas_list[0],
                addr_info->server_id_list[i], local_buf_mr_->lkey, remote_slot_addr, 
                (uint64_t)bucket_buffer->local_cas_return_value_addr,
                *(uint64_t *)old_local_slot_addr, *(uint64_t *)new_local_slot_ptr
                );
            sr_list = gen_cas_sr_lists_add_id(ctx->coro_id, 
                bucket_buffer->kv_modify_pr_cas_list, 
                &sr_list_num, CAS_PL_bucket);
        } else {
            sr_list = gen_one_sr_list((uint64_t)new_local_slot_ptr, sizeof(uint64_t), local_buf_mr_->lkey, 
                ctx->coro_id, addr_info->server_id_list[i], UPDATE_WRITE_PL_BUCKET, i, IBV_WR_RDMA_WRITE,
                remote_slot_addr, 0, NULL);
        }
        if(if_req_latency){
            if(i == 0){
                ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_CAS_PL_SLOT1;
            } else if(i == 1){
                ctx->wr_id_map_op_id[sr_list->sr_list->wr_id] = UPDATE_CAS_PL_SLOT2;
            }
        }
        kv_update_send(ctx->uctx, sr_list, 1);
    }
}

void Client::kv_update_check_write_PL_and_write_kv(KVReqCtx * ctx){
    IbvSrList *sr_list = ctx->sr_list_cache;
    UpdateCtx *uctx = ctx->uctx;
    if (*(uint64_t *)sr_list->sr_list->sg_list->addr == sr_list->sr_list->wr.atomic.compare_add) {
        ctx->ret_val.ret_code = KV_OPS_SUCCESS;
        ctx->is_finished = true;
    } else {
        // print_error("update workflow:RTT4 cas kv slot failed~");
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        ctx->is_finished = true;
        code_num_count.update_RTT4_cas_kv_failed ++;
        uctx->cas_rec_ele->recovery_times = CAS_FAILED_RECOVERY_TIMES;
        uctx->cas_rec_ele->recovery_when = rand() % RAND_RECOVERY_TIMES;
        cas_rec->insert(uctx->coro_id, uctx->cas_rec_ele);
    }
}

void Client::kv_update_RTT4_sync(KVReqCtx * ctx) {
    kv_update_init_sr_list(ctx);
    kv_update_RTT4_PL(ctx);
    if(ctx->is_finished == true){
        cout << "cas PL failed~" << endl;
        return;
    }
    kv_update_RTT4_kv(ctx);
    nm_->rdma_post_check_sr_list(ctx->uctx->comp_wrid_map);
    // 3. check update success
    // kv_update_check_write_PL_and_write_kv(ctx);
}

void Client::kv_update_RTT4_async(KVReqCtx * ctx) {
    kv_update_init_sr_list(ctx);
    kv_update_RTT4_PL(ctx);
    if(ctx->is_finished == true){
        cout << "cas PL failed~" << endl;
        return;
    }
    kv_update_RTT4_kv(ctx);
    kv_update_post_and_wait(ctx);
    // 3. check update success
    // kv_update_check_write_PL_and_write_kv(ctx);
}

void Client::get_local_bucket_info_kv(KVReqCtx * ctx) {
    ctx->f_com_bucket = ctx->local_bucket_addr;
    ctx->bucket_arr[0] = ctx->f_com_bucket;
    ctx->bucket_arr[1] = ctx->f_com_bucket + 2;
    ctx->slot_arr[0] = ctx->f_com_bucket[0].slots;
    ctx->slot_arr[1] = ctx->f_com_bucket[1].slots;
}

void Client::init_count_used_slot(){
    count_used_kv_slot = new int[KV_ALL_USDE_SLOT];
    for(int i = 0;i < KV_ALL_USDE_SLOT;i ++){
        count_used_kv_slot[i] = 0;
    }
}

void Client::print_count_used_slot(){
    print_mes("print count used slot-------------------------------");
    for(int i = 0;i < KV_ALL_USDE_SLOT;i ++){
        cout << "num of used slot: " << i << " :" << count_used_kv_slot[i] << endl;
    }
    print_mes("print count used slot-------------------------------- finished~");
}

void Client::find_empty_kv_slot_crash(KVReqCtx * ctx){
    get_local_bucket_info_kv(ctx);
    uint32_t f_free_num;
    uint32_t f_free_slot_idx;
    int32_t bucket_idx = -1;
    int32_t slot_idx = -1;
    int all_free_num = 0;
    int slot_i;
    for (int i = 0; i < 2; i ++) {
        f_free_num = get_free_kv_ec_meta_slot_num_crash((KvEcMetaBucket *)ctx->bucket_arr[i], 
            &f_free_slot_idx, ctx->hash_info.fp, slot_i);
        if(f_free_num == -2){
            bucket_idx = i;
            slot_idx = slot_i;
            break;
        }
        all_free_num += f_free_num;
        if (f_free_num > 0) {
            bucket_idx = i;
            slot_idx = f_free_slot_idx;
        }
    }
    ctx->bucket_idx = bucket_idx;
    ctx->slot_idx   = slot_idx;
}

void Client::find_empty_kv_slot(KVReqCtx * ctx){
    get_local_bucket_info_kv(ctx);
    uint32_t f_free_num;
    uint32_t f_free_slot_idx;
    int32_t bucket_idx = -1;
    int32_t slot_idx = -1;
    int all_free_num = 0;
    for (int i = 0; i < 2; i ++) {
        f_free_num = get_free_kv_ec_meta_slot_num((KvEcMetaBucket *)ctx->bucket_arr[i], 
            &f_free_slot_idx);
        all_free_num += f_free_num;
        if (f_free_num > 0) {
            bucket_idx = i;
            slot_idx = f_free_slot_idx;
        }
    }
    count_used_kv_slot[RACE_HASH_ASSOC_NUM * 2 - all_free_num] ++;
    ctx->bucket_idx = bucket_idx;
    ctx->slot_idx   = slot_idx;
}

bool Client::find_kv_slot_in_buckets(KvEcMetaBucket *bucket, KVRWAddr *rw, 
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

void Client::find_kv_slot(KVReqCtx * ctx) {
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

int32_t Client::find_match_kv_idx(KVReqCtx * ctx) {
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

void Client::fill_slot(ClientMMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info,
        __OUT RaceHashSlot * local_slot) {
    local_slot->fp = a_kv_hash_info->fp;
    local_slot->kv_len = mm_alloc_ctx->num_subblocks;
    local_slot->server_id = mm_alloc_ctx->server_id_list;
    hash_index_convert_64_to_40_bits(mm_alloc_ctx->addr_list, local_slot->pointer);
}

pthread_t Client::start_polling_thread() {
    NMPollingThreadArgs * args = (NMPollingThreadArgs *)malloc(sizeof(NMPollingThreadArgs));
    args->nm = nm_;
    args->core_id = poll_core_id_;
    pthread_t polling_tid;
    pthread_create(&polling_tid, NULL, nm_polling_thread, (void *)args);
    return polling_tid;
}

void Client::stop_polling_thread() {
    nm_->stop_polling();
}

void * client_ops_fb_cnt_time(void * arg) {
    boost::this_fiber::yield();
    ClientFiberArgs * fiber_args = (ClientFiberArgs *)arg;
    fiber_args->client->init_kvreq_space(fiber_args->coro_id, fiber_args->ops_st_idx, fiber_args->ops_num);
    uint32_t num_failed = 0;
    int ret = 0;
    void * search_addr = NULL;
    fiber_args->client->print_args("coro", fiber_args->coro_id);
    gettimeofday(fiber_args->st, NULL);
    for (int i = 0; i < fiber_args->ops_num; i ++) {
        KVReqCtx * ctx = &fiber_args->client->kv_req_ctx_list_[i + fiber_args->ops_st_idx];
        ctx->coro_id = fiber_args->coro_id;
        ctx->should_stop = fiber_args->should_stop;
        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            search_addr = fiber_args->client->kv_search(ctx);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            ret = fiber_args->client->kv_insert(ctx);
            if (ret == KV_OPS_FAIL_REDO || ret == KV_OPS_FAIL_RETURN) {
                num_failed++;
            }
            break;
        case KV_REQ_UPDATE:
            fiber_args->client->kv_update(ctx);
            break;
        case KV_REQ_DELETE:
            fiber_args->client->kv_delete(ctx);
            break;
        default:
            fiber_args->client->kv_search(ctx);
            break;
        }
    }
    gettimeofday(fiber_args->et, NULL);
    fiber_args->num_failed = num_failed;
    return NULL;
}

void init_num_count(NumCount & num_count){
    num_count.update_RTT2_read_stripe_meta_failed = 0;
    num_count.update_RTT2_read_wl_is_15 = 0;
    num_count.update_RTT3_faa_return_invalied = 0;
    num_count.update_RTT3_cas_return_invalied = 0;
    num_count.update_find_kv_failed = 0;
    num_count.update_match_kv_failed = 0;
    num_count.update_recovery_kv_num = 0;
    num_count.update_RTT4_cas_kv_failed = 0;
    num_count.search_find_failed = 0;
    num_count.search_match_failed = 0;
    num_count.cache_hit = 0;
    num_count.cache_hit_but_failed = 0;
    num_count.cache_miss = 0;
    num_count.num_search = 0;
    num_count.num_update = 0;
    num_count.num_insert = 0;
    num_count.num_degrade = 0;
}

void add_num_count(NumCount & init_num_count, NumCount & new_num_count){
    init_num_count.update_RTT2_read_stripe_meta_failed += new_num_count.update_RTT2_read_stripe_meta_failed;
    init_num_count.update_RTT2_read_wl_is_15 += new_num_count.update_RTT2_read_wl_is_15;
    init_num_count.update_RTT3_faa_return_invalied += new_num_count.update_RTT3_faa_return_invalied;
    init_num_count.update_RTT3_cas_return_invalied += new_num_count.update_RTT3_cas_return_invalied;
    init_num_count.update_RTT4_cas_kv_failed += new_num_count.update_RTT4_cas_kv_failed;
    init_num_count.cache_hit += new_num_count.cache_hit;
    init_num_count.cache_hit_but_failed += new_num_count.cache_hit_but_failed;
    init_num_count.cache_miss += new_num_count.cache_miss;
    init_num_count.update_find_kv_failed += new_num_count.update_find_kv_failed;
    init_num_count.update_match_kv_failed += new_num_count.update_match_kv_failed;
    init_num_count.update_recovery_kv_num += new_num_count.update_recovery_kv_num;
    init_num_count.search_find_failed += new_num_count.search_find_failed;
    init_num_count.search_match_failed += new_num_count.search_match_failed;
    init_num_count.num_search += new_num_count.num_search;
    init_num_count.num_update += new_num_count.num_update;
    init_num_count.num_insert += new_num_count.num_insert;
    init_num_count.num_degrade += new_num_count.num_degrade;
}

void print_num_count(NumCount code_num_count){
    printf("print num count ---------------------------\n");
    printf("RTT2_read_stripe_meta_failed failed: %d\n", code_num_count.update_RTT2_read_stripe_meta_failed);
    printf("RTT2_read_wl_is_15 failed: %d\n", code_num_count.update_RTT2_read_wl_is_15);
    printf("RTT3_faa_return_invalied failed: %d\n", code_num_count.update_RTT3_faa_return_invalied);
    printf("RTT3_cas_return_invalied failed: %d\n", code_num_count.update_RTT3_cas_return_invalied);
    printf("update_RTT4_cas_kv_failed failed: %d\n", code_num_count.update_RTT4_cas_kv_failed);
    printf("cache_hit : %d\n", code_num_count.cache_hit);
    printf("cache_hit_but_failed : %d\n", code_num_count.cache_hit_but_failed);
    printf("cache_miss : %d\n", code_num_count.cache_miss);
    printf("num_search : %d\n", code_num_count.num_search);
    printf("num_update : %d\n", code_num_count.num_update);
    printf("num_insert : %d\n", code_num_count.num_insert);
    printf("num_degrade : %d\n", code_num_count.num_degrade);
    printf("update_find_kv_failed failed: %d\n", code_num_count.update_find_kv_failed);
    printf("update_match_kv_failed failed: %d\n", code_num_count.update_match_kv_failed);
    printf("update_recovery_kv_num failed: %d\n", code_num_count.update_recovery_kv_num);
    printf("search_find_failed failed: %d\n", code_num_count.search_find_failed);
    printf("search_match_failed failed: %d\n", code_num_count.search_match_failed);
    printf("print num count -------------------finished~\n");
}

int get_all_num_failed(NumCount code_num_failed){
    return code_num_failed.update_RTT2_read_stripe_meta_failed + code_num_failed.update_RTT2_read_wl_is_15 + 
        code_num_failed.update_RTT3_cas_return_invalied + code_num_failed.update_RTT3_faa_return_invalied;
}

void * client_ops_fb_cnt_ops_cont(void * arg) {
    boost::this_fiber::yield();
    ClientFiberArgs * fiber_args = (ClientFiberArgs *)arg;

    RDMA_LOG_IF(4, fiber_args->client->if_print_log) << "start opertion fiber!";
    if (fiber_args->ops_cnt == 0) {
        fiber_args->client->init_kvreq_space(fiber_args->coro_id, fiber_args->ops_st_idx, fiber_args->ops_num);
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
        KVReqCtx * ctx = &fiber_args->client->kv_req_ctx_list_[idx + fiber_args->ops_st_idx];
        ctx->should_stop = fiber_args->should_stop;
        ctx->pre_ctx_index = idx;
        // cas kv slot failed reoperation
        // for(int i = 0;i < 1;i ++){
        //     CasRecoveryEle *cas_rec_ele = fiber_args->client->cas_rec->check_coro(fiber_args->coro_id);

        //     if(cas_rec_ele != NULL){
        //         ret = fiber_args->client->kv_update_cas_rec(ctx, cas_rec_ele);
        //     } else {
        //         break;
        //     }
            
        //     if (ret == KV_OPS_FAIL_RETURN) {
        //         num_failed ++;
        //     }
        //     count ++;
        // }
        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            // break;
            fiber_args->client->code_num_count.num_search ++;
            search_addr = fiber_args->client->kv_search(ctx);
            if (search_addr == NULL) {
                num_failed ++;
            }
            // char value[32];
            // memcpy(value, search_addr, 32);
            // RDMA_LOG_IF(2, fiber_args->client->if_print_log) << "search value:" << value;
            count ++;
            break;
        case KV_REQ_INSERT:
            fiber_args->client->code_num_count.num_insert ++;
            if (inserted_key_map[ctx->key_str] == true) {
                char * modify = (char *)((uint64_t)(ctx->kv_info->l_addr) + sizeof(KvKeyLen) + sizeof(KvValueLen));
                modify[2] ++;
                ctx->key_str[2] ++;
            }
            ret = fiber_args->client->kv_insert(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                // cout << "insert failed~" << endl;
                num_failed ++;
            } else {
                inserted_key_map[ctx->key_str] = true;
            }
            // inserted_key_map[ctx->key_str] = true;
            count ++;
            break;
        case KV_REQ_UPDATE:
            // break;
            fiber_args->client->code_num_count.num_update ++;
            ret = fiber_args->client->kv_update(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            count ++;
            // cnt --;
            break;
        case KV_REQ_DELETE:
            ret = fiber_args->client->kv_delete(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            count ++;
            break;
        case KV_DEGRADE_READ:
            fiber_args->client->code_num_count.num_degrade ++;
            search_addr = fiber_args->client->kv_degrade_read(ctx);
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
        default:
            fiber_args->client->kv_search(ctx);
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

void Client::get_recover_time(std::vector<struct timeval> & recover_time) {
    recover_time.push_back(recover_st_);
    recover_time.push_back(connection_recover_et_);
    recover_time.push_back(mm_recover_et_);
    recover_time.push_back(local_mr_reg_et_);
    recover_time.push_back(ec_init_et_);
    recover_time.push_back(middle_et_);
    recover_time.push_back(get_wp_rp_middle_et_);
    recover_time.push_back(get_wp_rp_et_);
    recover_time.push_back(get_unicol_log_et);
    recover_time.push_back(kv_ops_recover_et_);
}

void Client::init_mem_con(){
    mem_con.kv_data = 0;
    mem_con.kv_ec_meta = 0;
    mem_con.metadatablock = 0;
    mem_con.parity_data = 0;
    mem_con.stripe_meta = 0;
    mem_con.log = 0;
}

void Client::print_mem_con(){
    cout << "print mem con:" << endl;
    cout << "kv data:" << mem_con.kv_data << endl;
    cout << "parity data:" << mem_con.parity_data << endl;
    cout << "metadatablock:" << mem_con.metadatablock << endl;
    cout << "stripe meta:" << mem_con.stripe_meta << endl;
    cout << "kv ec meta:" << mem_con.kv_ec_meta << endl;
    cout << "log:" << mem_con.log << endl;
    cout << "-----------------" << endl;
    cout << "data cost:" << (mem_con.kv_data + mem_con.parity_data) * 2048 << endl;
    cout << "ec metadata cost:" << (mem_con.metadatablock + mem_con.stripe_meta + mem_con.kv_ec_meta) * 2048 << endl;
    cout << "log cost:" << mem_con.log << endl;
    cout << "all cost:" << (mem_con.kv_data + mem_con.parity_data + mem_con.metadatablock + 
        mem_con.stripe_meta + mem_con.kv_ec_meta) * 2048 + mem_con.log << endl;
    cout << "print mem con finished~" << endl;
}

void Client::client_init_req_latency(int num_ops, int op_type){
    req_latency = new ReqLatency(num_ops, op_type);
}

void Client::client_add_req_latency(ReqTimer *req_timer){
    req_latency->add_req_latency(req_timer);
}

void Client::client_print_req_latency(char *op){
    char out_fname[128];
    FILE *of;
    sprintf(out_fname, "results/op_%s-clients%d-threads%d-breakdown.txt", op,
        all_clients * num_cn, client_id);
    print_mes("Start to write latency breakdown~");
    of = fopen(out_fname, "w");
    cout << "of:" << of << endl;
    for(int i = 0;i < req_latency->pre_req;i ++){
        for(int j = 0;j < req_latency->num_ops;j ++){
            fprintf(of, "%f", req_latency->latency_list[i][j]);
            if(j != req_latency->num_ops - 1){
                fprintf(of, " ");
            }
        }
        if(i != req_latency->pre_req - 1){
            fprintf(of, "\n");
        }
    }
    print_mes("Write latency breakdown finished~");
}

ReqTimer::ReqTimer(int op_type){
    num_ops = get_num_ops_from_type(op_type);
    timer_list = new OneTimer[num_ops];
}

ReqTimer::~ReqTimer(){
    delete [] timer_list;
}

void ReqTimer::start_timer(int bd){
    clock_gettime(CLOCK_REALTIME, &timer_list[bd].start_time);
}

void ReqTimer::end_timer(int bd){
    clock_gettime(CLOCK_REALTIME, &timer_list[bd].end_time);
}

ReqLatency::ReqLatency(int num_req, int op_type){
    this->op_type = op_type;
    this->num_ops = get_num_ops_from_type(op_type);
    latency_list = new double*[num_req];
    for(int i = 0;i < num_req;i ++){
        latency_list[i] = new double[num_ops];
    }
    this->num_req = num_req;
    pre_req = 0;
}

ReqLatency::~ReqLatency(){
    for(int i = 0;i < num_req;i ++){
        delete [] latency_list[i];
    }
    delete [] latency_list;
}

void ReqLatency::add_req_latency(ReqTimer *req_timer){
    for(int i = 0;i < num_ops;i ++){
        latency_list[pre_req][i] = get_timer_spec_us(&req_timer->timer_list[i]);
    }
    pre_req ++;
}

void ReqLatency::print_all_latency(){
    
}

double get_timer_spec_us(OneTimer *timer){
    return (double)((timer->end_time.tv_sec - timer->start_time.tv_sec) * 1000000000 + 
        (timer->end_time.tv_nsec - timer->start_time.tv_nsec)) / 1000.0;
}

int get_num_ops_from_type(int op_type){
    switch(op_type){
        case KV_REQ_UPDATE:
            return NUM_UPDATE_OPS;
        default:
            return NUM_UPDATE_OPS;
    }
}

void Client::rdma_test_req_read(KVReqCtx *ctx){
    ctx->comp_wrid_map.clear();

    int test_size = sizeof(uint64_t);

    uint64_t local_addr, remote_addr;
    int num_test_ = 100;
    for(int i = 0;i < num_test_;i ++){
        local_addr = (uint64_t)ctx->kv_info->l_addr + test_size * i;
        remote_addr = remote_sync_addr + test_size *
            (((client_id - num_memory_) * num_coroutines_ + ctx->coro_id) * num_test + i);

        IbvSrList *sr_list = gen_one_sr_list(local_addr, test_size, input_buf_mr_->lkey,
            ctx->coro_id, i % num_memory_, READ_KV, ctx->pre_ctx_index + i, IBV_WR_RDMA_READ, remote_addr,
            0, NULL);
        send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    }
    int ret = poll_completion(ctx->comp_wrid_map);
    if(ret == -1){
        print_error("rdma test req read failed~");
        exit(0);
    }
}

void Client::race_insert_RTT1(KVReqCtx *ctx){
    int ret = 0;
    ctx->comp_wrid_map.clear();
    uint32_t kv_block_size = ctx->kv_all_len;
    uint64_t server_id = ctx->tbl_addr_info.server_id_list[0];
    mm_->mm_alloc(kv_block_size, nm_, server_id, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list < server_st_addr_ || 
        ctx->mm_alloc_ctx.addr_list >= server_st_addr_ + server_data_len_) {
        print_error("insert workflow:read_buckets_and_write_kv_sync:mm alloc failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &sr_list_num);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, server_id, READ_BUCKET_ST_WRID, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    send_one_sr_list(sr_list, &ctx->comp_wrid_map);
    ret = nm_->rdma_post_check_sr_list(&ctx->comp_wrid_map);
    if(ret == -1){
        print_error("insert workflow:read_buckets_and_write_kv send sr list failed~");
        exit(0);
    }
    ctx->is_finished = false;
    return;
}

void Client::race_insert_RTT2(KVReqCtx *ctx){
    int ret = 0;
    find_empty_kv_slot(ctx); 
    if (ctx->bucket_idx == -1) {
        // print_error("insert workflow:find kv buckets failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    ctx->comp_wrid_map.clear();
    RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
    fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
    uint64_t sub = KV_BUCKET_SIZE * ctx->bucket_idx + sizeof(KvEcMetaSlot) * ctx->slot_idx + sizeof(uint64_t) * 2;
    uint64_t remote_slot_addr;
    remote_slot_addr = ctx->tbl_addr_info.f_bucket_addr[0] + sub; 
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    vector<IbvSrList *> sr_list_batch;
    vector<uint32_t> sr_list_num_batch;
    sr_list = gen_one_cas_sr_list((uint64_t)ctx->local_cas_return_value_addr, local_buf_mr_->lkey, ctx->coro_id, 
        ctx->tbl_addr_info.server_id_list[0], CAS_ST_WRID, 0, remote_slot_addr, 0, *(uint64_t *)new_local_slot_ptr,
        IBV_SEND_SIGNALED, NULL);
    sr_list_batch.push_back(sr_list);
    sr_list_num_batch.push_back(1);
    struct ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(sr_list_batch, sr_list_num_batch, &wc);
    if(ret == -1){
        print_error("insert workflow:write_buckets send sr list failed~");
    }
    // check update success
    if (*(uint64_t *)ctx->local_cas_return_value_addr != 0) {
        RDMA_LOG_IF(2, if_print_log) << "server:" << client_id << " " << "kv_insert_write_buckets check failed";
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        ctx->is_finished = true;
        return;
    }
    ctx->is_finished = true;
    ctx->ret_val.ret_code = 0;
    return;
}

int Client::race_insert(KVReqCtx *ctx){
    race_insert_RTT1(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    race_insert_RTT2(ctx);
    return ctx->ret_val.ret_code;
}

void Client::race_search_RTT1(KVReqCtx *ctx){
    uint32_t sr_list_num;
    IbvSrList *sr_list;
    sr_list = gen_one_sr_list_batch((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, ctx->tbl_addr_info.server_id_list[0], READ_BUCKET_ST_WRID, 0, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    vector<IbvSrList *> *sr_list_batch = new vector<IbvSrList *>;
    vector<uint32_t> *sr_list_num_batch = new vector<uint32_t>;
    sr_list_batch->push_back(sr_list);
    sr_list_num_batch->push_back(1);
    struct ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(*sr_list_batch, *sr_list_num_batch, &wc);
    free_sr_list(sr_list, 1);
    delete sr_list_batch;
    delete sr_list_num_batch;
    ctx->is_finished = false;
    return;
}

void Client::race_search_RTT2(KVReqCtx *ctx){
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
    vector<IbvSrList *> *sr_list_batch = new vector<IbvSrList *>;
    vector<uint32_t> *sr_list_num_batch = new vector<uint32_t>;
    sr_list = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &sr_list_num);
    sr_list_batch->push_back(sr_list);
    sr_list_num_batch->push_back(sr_list_num);
    struct ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(*sr_list_batch, *sr_list_num_batch, &wc);
    delete sr_list_batch;
    delete sr_list_num_batch;
    free_sr_list(sr_list, 1);
    ctx->is_finished = false;
    return;
}

void *Client::race_search(KVReqCtx *ctx){
    race_search_RTT1(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.value_addr;
    }
    race_search_RTT2(ctx); 
    kv_search_check_kv(ctx);
    return ctx->ret_val.value_addr;
}

void Client::race_update_RTT1(KVReqCtx *ctx){
    ctx->is_finished = false;
    kv_update_init_sr_list(ctx);
    UpdateCtx *uctx = ctx->uctx;
    uint32_t kv_block_size = ctx->kv_all_len;
    uint64_t server_id = ctx->tbl_addr_info.server_id_list[0];
    mm_->mm_alloc(kv_block_size, nm_, server_id, &ctx->mm_alloc_ctx);
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
    uctx->sr_list_batch.push_back(sr_list);
    uctx->sr_list_num_batch.push_back(1);
    sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &sr_list_num, WRITE_KV_ST_WRID);
    uctx->sr_list_batch.push_back(sr_list);
    uctx->sr_list_num_batch.push_back(1);
    ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(uctx->sr_list_batch, uctx->sr_list_num_batch, &wc);
}

void Client::race_update_RTT2(KVReqCtx *ctx){
    kv_update_init_sr_list(ctx);\
    UpdateCtx *uctx = ctx->uctx;
    find_kv_slot(ctx);
    if(ctx->is_finished == true){
        return;
    }
    if(ctx->kv_read_addr_list.size() == 0){
        print_error("update workflow:read kv:find kv in buckets failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList * sr_list = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &sr_list_num);
    uctx->sr_list_batch.push_back(sr_list);
    uctx->sr_list_num_batch.push_back(1);
    ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(uctx->sr_list_batch, uctx->sr_list_num_batch, &wc);
}

void Client::race_update_RTT3(KVReqCtx *ctx){
    ctx->uctx->PL_match_idx = find_match_kv_idx(ctx);
    if (ctx->uctx->PL_match_idx == -1) {
        // print_error("update workflow:find kv failed in prepare PL info~");
        code_num_count.cache_miss ++;
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    kv_update_init_sr_list(ctx);
    if(if_req_latency){
        ctx->req_timer->start_timer(UPDATE_CAS_KV_SLOT);
    }
    std::pair<int32_t, int32_t> idx_pair = ctx->kv_idx_list[ctx->uctx->PL_match_idx];
    int32_t bucket_idx = idx_pair.first;
    int32_t slot_idx   = idx_pair.second;
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
        IBV_SEND_SIGNALED, NULL);
    UpdateCtx *uctx = ctx->uctx;
    uctx->sr_list_batch.push_back(sr_list);
    uctx->sr_list_num_batch.push_back(1);
    ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(uctx->sr_list_batch, uctx->sr_list_num_batch, &wc);
    // send_one_sr_list(sr_list, uctx->comp_wrid_map);
    // nm_->rdma_post_check_sr_list(uctx->comp_wrid_map);
}

int Client::race_update(KVReqCtx *ctx){
    race_update_RTT1(ctx);
    race_update_RTT2(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.ret_code;
    }
    race_update_RTT3(ctx);
    return ctx->ret_val.ret_code;
}

void Client::race_delete_RTT1(KVReqCtx *ctx){
    ctx->is_finished = false;
    kv_delete_init_sr_list(ctx->dctx);
    // prepare write kv and read bucket sr lists
    uint32_t sr_list_num;
    IbvSrList * sr_list;
    uint8_t server_id = ctx->tbl_addr_info.server_id_list[0];
    sr_list = gen_one_sr_list((uint64_t)ctx->local_bucket_addr, KV_BUCKET_SIZE * 2, local_buf_mr_->lkey,
        ctx->coro_id, server_id, READ_KV_BUCKET, 1, IBV_WR_RDMA_READ, ctx->tbl_addr_info.f_bucket_addr[0],
        0, NULL);
    DeleteCtx *dctx = ctx->dctx;
    std::vector<IbvSrList *> sr_list_batch;
    std::vector<uint32_t> sr_list_num_batch;
    sr_list_batch.push_back(sr_list);
    sr_list_num_batch.push_back(1);
    ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(sr_list_batch, sr_list_num_batch, &wc);
}

void Client::race_delete_RTT2(KVReqCtx *ctx){
    kv_delete_init_sr_list(ctx->dctx);
    find_kv_slot(ctx);
    if(ctx->is_finished == true){
        return;
    }
    if(ctx->kv_read_addr_list.size() == 0){
        print_error("update workflow:read kv:find kv in buckets failed~");
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    uint32_t sr_list_num;
    IbvSrList * sr_list = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &sr_list_num);
    DeleteCtx *dctx = ctx->dctx;
    std::vector<IbvSrList *> sr_list_batch;
    std::vector<uint32_t> sr_list_num_batch;
    sr_list_batch.push_back(sr_list);
    sr_list_num_batch.push_back(1);
    ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(sr_list_batch, sr_list_num_batch, &wc);
}

void Client::race_delete_RTT3(KVReqCtx *ctx){
    int if_match = find_match_kv_idx(ctx);
    if (if_match == -1) {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    kv_delete_init_sr_list(ctx->dctx);
    DeleteCtx *dctx = ctx->dctx;
    int32_t bucket_idx = ctx->bucket_idx;
    int32_t slot_idx   = ctx->slot_idx;
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
        IBV_SEND_SIGNALED, NULL);
    std::vector<IbvSrList *> sr_list_batch;
    std::vector<uint32_t> sr_list_num_batch;
    sr_list_batch.push_back(sr_list);
    sr_list_num_batch.push_back(1);
    ibv_wc wc;
    nm_->rdma_post_sr_list_batch_sync(sr_list_batch, sr_list_num_batch, &wc);
    if (*(uint64_t *)ctx->local_cas_return_value_addr == ctx->old_value) {
        ctx->ret_val.ret_code = KV_OPS_SUCCESS;
    } else {
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
    }
}

int Client::race_delete(KVReqCtx *ctx){
    race_delete_RTT1(ctx);
    race_delete_RTT2(ctx);
    if(ctx->is_finished == true){
        return ctx->ret_val.ret_code;
    }
    race_delete_RTT3(ctx);
    return ctx->ret_val.ret_code;
}
