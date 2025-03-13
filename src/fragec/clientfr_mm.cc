#include "clientfr_mm.h"

ClientFRMM::ClientFRMM(const struct GlobalConfig * conf, UDPNetworkManager * nm) {

    client_id = conf->server_id;
    server_limit_addr_ = conf->server_base_addr + conf->server_data_len;
    num_memory_      = conf->memory_num;
    last_allocated_ = conf->server_id;
    mm_block_sz_ = conf->allocate_size;
    subblock_sz_ = conf->subblock_size;
    subblock_num_ = mm_block_sz_ / subblock_sz_;
    bmap_block_num_ = subblock_num_ / 8 / subblock_sz_;
    if (bmap_block_num_ * 8 * subblock_sz_ < subblock_num_)
        bmap_block_num_ += 1;

    subblock_free_queue_ = new deque<SubblockInfo>[num_memory_];
    subblock_free_queue_encoding_ = new deque<SubblockInfo>[num_memory_];

    // allocate initial blocks
    init_get_new_block_from_server(nm);
   
}

ClientFRMM::~ClientFRMM() {
    delete [] subblock_free_queue_;
    delete [] subblock_free_queue_encoding_;
}

void ClientFRMM::check_subblock(){
    for(int i = 0;i < num_memory_;i ++){
        cout << "size:" << subblock_free_queue_[i].size() << endl;
    }
}

int32_t ClientFRMM::dyn_get_new_block_from_server(UDPNetworkManager * nm, uint64_t server_id, int is_enc) {

    struct MrInfo mr_info_list;
    uint8_t server_id_list = server_id;
    // get one kv block addr from server
    alloc_from_sid(server_id, nm, TYPE_KVBLOCK, &mr_info_list); 
    if (mr_info_list.addr == 0) {
        return -1;
    }
    if ((mr_info_list.addr & 0xFF) != 0) {
        printf("error addr: %lx\n", mr_info_list.addr);
        exit(1);
    }
    // allocate remote memory
    dyn_reg_new_space(&mr_info_list, &server_id_list, nm, TYPE_KVBLOCK, is_enc);

    return 0;
}

int ClientFRMM::init_get_new_block_from_server(UDPNetworkManager * nm) {
    
    struct MrInfo mr_info_list[num_memory_];
    uint8_t server_id_list[num_memory_];

    for (int i = 0; i < num_memory_; i ++) {
        uint8_t pr_server_id = i;
        server_id_list[i] = pr_server_id;
        // get one kv block addr from server
        alloc_from_sid(pr_server_id, nm, TYPE_KVBLOCK, &mr_info_list[i]);
        if ((mr_info_list[i].addr & 0xFF) != 0) {
            printf("error addr: %lx\n", mr_info_list[i].addr);
            exit(1);
        }
    }

    // allocate all remote memory
    init_reg_space(mr_info_list, server_id_list, nm, TYPE_KVBLOCK);
    return 0;
}

void ClientFRMM::mm_alloc(size_t size, UDPNetworkManager * nm, uint64_t server_id,  __OUT ClientMMAllocCtx * ctx) {

    int ret = 0;
    size_t aligned_size = get_aligned_size(size);
    int num_subblocks_required = aligned_size / subblock_sz_; 
    if(num_subblocks_required > subblock_free_queue_[server_id].size()){ 
        // dynamic get one kv block from a memory node
        ret = dyn_get_new_block_from_server(nm, server_id, KV_BLOCK_TYPE);
        if (ret == -1) {
            cout << "server:" << server_id << " have no space chunk" << endl;
            ctx->addr_list = 0;
            return;
        }
    }

    // allocate from local subblocks
    SubblockInfo alloc_subblock = subblock_free_queue_[server_id].front();
    for(int i = 0;i < num_subblocks_required;i ++){
        subblock_free_queue_[server_id].pop_front();
    }

    // assign some message
    ctx->num_subblocks = num_subblocks_required;
    ctx->addr_list = alloc_subblock.addr_list;
    ctx->rkey_list = alloc_subblock.rkey_list;
    ctx->server_id_list = alloc_subblock.server_id_list;
}

void ClientFRMM::mm_alloc_encoding(size_t size, UDPNetworkManager * nm, uint64_t server_id,  __OUT ClientMMAllocCtx * ctx) {

    int ret = 0;
    size_t aligned_size = get_aligned_size(size);
    int num_subblocks_required = aligned_size / subblock_sz_; 
    if(num_subblocks_required > subblock_free_queue_encoding_[server_id].size()){ 
        subblock_free_queue_encoding_[server_id].clear();
        // dynamic get one kv block from a memory node
        ret = dyn_get_new_block_from_server(nm, server_id, ENCODING_BLOCK);
        if (ret == -1) {
            cout << "server:" << server_id << " have no encoding space chunk" << endl;
            ctx->addr_list = 0;
            return;
        }
    }

    SubblockInfo alloc_subblock = subblock_free_queue_encoding_[server_id].front();
    for(int i = 0;i < num_subblocks_required;i ++){
        subblock_free_queue_encoding_[server_id].pop_front();
    }
    
    ctx->num_subblocks = num_subblocks_required;
    ctx->addr_list = alloc_subblock.addr_list;
    ctx->rkey_list = alloc_subblock.rkey_list;
    ctx->server_id_list = alloc_subblock.server_id_list;

}

void ClientFRMM::mm_alloc_small_subtable(UDPNetworkManager * nm, __OUT ClientMMAllocSubtableCtx * ctx, uint8_t server_id) {
    struct MrInfo mr_info_list;
    // get one small subtable from a memory node
    alloc_from_sid(server_id, nm, TYPE_SMALL_SUBTABLE, &mr_info_list);
    ctx->addr = mr_info_list.addr;
    ctx->server_id = server_id;
    if(ctx->addr == 0){
        cout << "server:" << ctx->server_id << " have no small subtable~" << endl;
    }
    // register new space
    reg_new_space(&mr_info_list, &server_id, nm, TYPE_SMALL_SUBTABLE);
}

void ClientFRMM::mm_alloc_big_subtable(UDPNetworkManager * nm, __OUT ClientMMAllocSubtableCtx * ctx, uint8_t server_id) {
    struct MrInfo mr_info_list;
    // get one big subtable from a memory node
    alloc_from_sid(server_id, nm, TYPE_BIG_SUBTABLE, &mr_info_list);
    ctx->addr = mr_info_list.addr;
    ctx->server_id = server_id;
    if(ctx->addr == 0){
        cout << "server:" << ctx->server_id << " have no big subtable~" << endl;
    }
    // register new space
    reg_new_space(&mr_info_list, &server_id, nm, TYPE_BIG_SUBTABLE);
}

void ClientFRMM::mm_alloc_log(UDPNetworkManager * nm, __OUT ClientMMAllocLogCtx * ctx) {
    uint32_t alloc_hint   = get_alloc_hint_rr();
    uint32_t pr_server_id = nm->get_one_server_id(alloc_hint);
    uint32_t num_servers  = nm->get_num_servers();
    struct MrInfo mr_info_list;
    uint32_t server_id = pr_server_id % num_servers;
    // get one log addr from a memory node
    alloc_from_sid(server_id, nm, TYPE_LOG, &mr_info_list);
    ctx->addr = mr_info_list.addr;
    ctx->server_id = server_id;
}

int ClientFRMM::alloc_from_sid(uint32_t server_id, UDPNetworkManager * nm, int alloc_type, __OUT struct MrInfo * mr_info) {
    struct KVMsg request, reply;
    memset(&request, 0, sizeof(struct KVMsg));
    memset(&reply, 0, sizeof(struct KVMsg));
    request.id = nm->get_server_id();
    if (alloc_type == TYPE_KVBLOCK) {
        request.type = REQ_ALLOC;
    } else if(alloc_type == TYPE_LOG) {
        request.type = REQ_ALLOC_LOG;
    } else if(alloc_type == TYPE_SMALL_SUBTABLE){
        request.type = REQ_ALLOC_SMALL_SUBTABLE;
    } else if(alloc_type == TYPE_BIG_SUBTABLE){
        request.type = REQ_ALLOC_BIG_SUBTABLE;
    }
    serialize_kvmsg(&request);
    request.client_id = client_id - num_memory_;
    // get a memory addr from server
    nm->nm_send_udp_msg_to_server(&request, server_id);
    nm->nm_recv_udp_msg(&reply, NULL, NULL);
    deserialize_kvmsg(&reply);
    memcpy(mr_info, &reply.body.mr_info, sizeof(struct MrInfo));
    return 0;
}

int ClientFRMM::local_reg_blocks(const struct MrInfo * mr_info_list, const uint8_t * server_id_list, int is_enc) {  
    ClientMMBlock * new_mm_block = (ClientMMBlock *)malloc(sizeof(ClientMMBlock));
    memset(new_mm_block , 0, sizeof(ClientMMBlock));
    memcpy(&new_mm_block->mr_info_list, mr_info_list, sizeof(struct MrInfo));
    new_mm_block->server_id_list = server_id_list[0];
    std::queue<SubblockInfo> tmp_queue;
    // add subblocks to subblock list
    // Start from bmap_block_num_ to pass the leading bit map blocks
    for (int i = bmap_block_num_; i < subblock_num_; i ++) {
        SubblockInfo tmp_info;
        tmp_info.addr_list = new_mm_block->mr_info_list.addr + i * subblock_sz_;
        tmp_info.rkey_list = new_mm_block->mr_info_list.rkey;
        tmp_info.server_id_list = new_mm_block->server_id_list;
        tmp_queue.push(tmp_info);
    }
    // allocate block
    uint32_t total_block_num = tmp_queue.size();
    uint32_t data_block_num;
    if(is_enc == INIT_KV){
        data_block_num = total_block_num / 10 * 9;
        subblock_free_queue_[server_id_list[0]].clear();
        subblock_free_queue_encoding_[server_id_list[0]].clear();
    } else if(is_enc == KV_BLOCK_TYPE){
        subblock_free_queue_[server_id_list[0]].clear();
        data_block_num = total_block_num;
    } else {
        subblock_free_queue_encoding_[server_id_list[0]].clear();
        data_block_num = 0;
    }
    
    uint32_t encoding_block_num = total_block_num - data_block_num;
    for (int i = 0; i < data_block_num; i ++) {
        SubblockInfo tmp_info = tmp_queue.front();
        if(tmp_info.server_id_list >= num_memory_){
            cout << "server id not match~" << endl;
        }
        if(tmp_info.server_id_list != tmp_info.server_id_list){
            cout << "server id not match~" << endl;
        }
        subblock_free_queue_[server_id_list[0]].push_back(tmp_info);
        tmp_queue.pop();
    }

    for (int i = 0; i < encoding_block_num; i ++) {
        SubblockInfo tmp_info = tmp_queue.front();
        if(tmp_info.server_id_list >= num_memory_){
            cout << "server id not match~" << endl;
        }
        if(tmp_info.server_id_list != tmp_info.server_id_list){
            cout << "server id not match~" << endl;
        }
        subblock_free_queue_encoding_[server_id_list[0]].push_back(tmp_info);
        tmp_queue.pop();
    }
    
    return 0;
}

int ClientFRMM::reg_new_space(const struct MrInfo * mr_info_list, const uint8_t * server_id_list, UDPNetworkManager * nm, int alloc_type) {
    // locally register blocks
    if (alloc_type == TYPE_KVBLOCK) {
        local_reg_blocks(mr_info_list, server_id_list, INIT_KV);
    }
    return 0;
}

int ClientFRMM::init_reg_space(struct MrInfo mr_info_list[], uint8_t server_id_list[], UDPNetworkManager * nm, int alloc_type) {
    uint8_t server_id;
    for (int m = 0; m < num_memory_; m ++) {
        ClientMMBlock * new_mm_block = (ClientMMBlock *)malloc(sizeof(ClientMMBlock));
        memset(new_mm_block, 0, sizeof(ClientMMBlock));
        memcpy(&new_mm_block->mr_info_list, &mr_info_list[m], sizeof(struct MrInfo));
        server_id = server_id_list[m];
        new_mm_block->server_id_list = server_id;
        // start from bmap_block_num to pass the leading bitmap blocks
        std::queue<SubblockInfo> tmp_queue;
        for (int i = bmap_block_num_; i < subblock_num_; i ++) {
            SubblockInfo tmp_info;
            tmp_info.addr_list = new_mm_block->mr_info_list.addr + i * subblock_sz_;
            tmp_info.rkey_list = new_mm_block->mr_info_list.rkey;
            tmp_info.server_id_list = server_id;
            tmp_queue.push(tmp_info);
        }
        // allocate block
        uint32_t total_block_num = tmp_queue.size();
        uint32_t data_block_num;

        subblock_free_queue_[server_id].clear();
        subblock_free_queue_encoding_[server_id].clear();

        data_block_num = total_block_num / 10 * 7;
        uint32_t encoding_block_num = total_block_num - data_block_num;

        for (int i = 0; i < data_block_num; i ++) {
            SubblockInfo tmp_info = tmp_queue.front();
            subblock_free_queue_[server_id].push_back(tmp_info);
            if(server_id != tmp_info.server_id_list){
                cout << "not match~" << endl;
            }
            tmp_queue.pop();
        }

        for (int i = 0; i < encoding_block_num; i ++) {
            SubblockInfo tmp_info = tmp_queue.front();
            subblock_free_queue_encoding_[server_id].push_back(tmp_info);
            tmp_queue.pop();
        }

    }
    return 0;
}

int ClientFRMM::dyn_reg_new_space(const struct MrInfo * mr_info_list, const uint8_t * server_id_list, UDPNetworkManager * nm, int alloc_type, int is_enc) {
    // locally register blocks
    if (alloc_type == TYPE_KVBLOCK) {
        local_reg_blocks(mr_info_list, server_id_list, is_enc);
    }
    return 0;
}

void ClientFRMM::print_leave_block(){
    RDMA_LOG(3) << "print block leave----------------";
    for(int i = 0;i < num_memory_;i ++){
        RDMA_LOG(3) << "server:" << i;
        RDMA_LOG(3) << "chunk leave:" << subblock_free_queue_[i].size();
        RDMA_LOG(3) << "encoding leave:" << subblock_free_queue_encoding_[i].size();
    }
    RDMA_LOG(3) << "print finish---------------------";
}

void ClientFRMM::get_time_bread_down(std::vector<struct timeval> & time_vec) {
    time_vec.push_back(local_recover_space_et_);
    time_vec.push_back(get_addr_meta_et_);
    time_vec.push_back(traverse_log_et_);
}