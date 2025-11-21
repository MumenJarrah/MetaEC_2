#include "nm.h"

int UDPNetworkManager::udp_init_client(const struct GlobalConfig * conf) {
    this->num_server_ = conf->memory_num;
    this->server_addr_list_ = (struct sockaddr_in *)malloc(
        this->num_server_ * sizeof(struct sockaddr_in));
    memset(this->server_addr_list_, 0, sizeof(struct sockaddr_in) * this->num_server_);
    this->rc_qp_list_.resize(conf->memory_num);
    this->mr_info_list_.reserve(conf->memory_num);
    // create deep cq for client
    if(this->ib_ctx_ == NULL){
        perror("ibv_create_ctx");
        exit(0);
    }
    this->ib_cq_ = ibv_create_cq(this->ib_ctx_, 1024, NULL, NULL, 0);
    if(this->ib_cq_ == NULL){
        perror("ibv_create_cq");
        exit(0);
    }
    for (int i = 0; i < conf->memory_num; i ++) {
        server_addr_list_[i].sin_family = AF_INET;
        server_addr_list_[i].sin_port   = htons(this->udp_port_);
        server_addr_list_[i].sin_addr.s_addr = inet_addr(conf->memory_ips[i]);
    }
    return 0;
}

int UDPNetworkManager::udp_init_server(const struct GlobalConfig * conf) {
    // set sock option
    int ret = 0;
    struct timeval timeout;
    timeout.tv_sec  = 1;
    timeout.tv_usec = 0;
    ret = setsockopt(udp_sock_, SOL_SOCKET, SO_RCVTIMEO, &timeout, 
        sizeof(struct timeval));
    num_server_       = 1;
    server_addr_list_ = (struct sockaddr_in *)malloc(num_server_ * sizeof(struct sockaddr_in));
    memset(server_addr_list_, 0, sizeof(struct sockaddr_in) * num_server_);
    server_addr_list_[0].sin_family = AF_INET;
    server_addr_list_[0].sin_port   = htons(udp_port_);
    server_addr_list_[0].sin_addr.s_addr = htonl(INADDR_ANY);
    // create shallow cq for server
    ib_cq_ = ibv_create_cq(ib_ctx_, 1, NULL, NULL, 0);
    ret = bind(udp_sock_, (struct sockaddr *)&server_addr_list_[0], sizeof(struct sockaddr_in));
    return 0;
}

UDPNetworkManager::UDPNetworkManager(const struct GlobalConfig * conf) {
    // initialize udp socket
    int ret = 0;
    udp_sock_ = socket(AF_INET, SOCK_DGRAM, 0);
    role_        = conf->role;
    server_id_   = conf->server_id;
    conn_type_   = conf->conn_type;
    udp_port_    = conf->udp_port;
    ib_port_num_ = conf->ib_port_id;
    stop_polling_ = false;
    // create ib structs
    ib_ctx_ = ib_get_ctx(conf->ib_dev_id, conf->ib_port_id);
    assert(ib_ctx_ != NULL);
    ib_pd_ = ibv_alloc_pd(this->ib_ctx_);
    assert(ib_pd_ != NULL);
    ret = ibv_query_port(ib_ctx_, conf->ib_port_id, &ib_port_attr_);
    ret = ibv_query_device(ib_ctx_, &ib_device_attr_);
    if (conn_type_ == ROCE) {
        ib_gid_ = (union ibv_gid *)malloc(sizeof(union ibv_gid));
        ret = ibv_query_gid(ib_ctx_, conf->ib_port_id, conf->ib_gid_idx, ib_gid_);
    } else {
        ib_gid_ = NULL;
    }
    // initialize client
    if (role_ == CLIENT) {
        ret = udp_init_client(conf);
    } else {
        ret = udp_init_server(conf);
    }
}

UDPNetworkManager::~UDPNetworkManager() {
    close(this->udp_sock_);
    free(server_addr_list_);
}

int UDPNetworkManager::nm_on_connect_new_qp(const struct KVMsg * request, 
    __OUT struct QpInfo * qp_info) {
    int rc = 0;
    struct ibv_qp * new_rc_qp = this->server_create_rc_qp();
    if (this->rc_qp_list_.size() <= request->id) {
        this->rc_qp_list_.resize(request->id + 1);
    }
    if (this->rc_qp_list_[request->id] != NULL) {
        ibv_destroy_qp(rc_qp_list_[request->id]);
    }
    this->rc_qp_list_[request->id] = new_rc_qp;
    rc = this->get_qp_info(new_rc_qp, qp_info);
    return 0;
}

int UDPNetworkManager::nm_on_connect_connect_qp(uint32_t client_id, 
    const struct QpInfo * local_qp_info, 
    const struct QpInfo * remote_qp_info) {
    int rc = 0;
    struct ibv_qp * qp = this->rc_qp_list_[client_id];
    rc = ib_connect_qp(qp, local_qp_info, remote_qp_info, 
        this->conn_type_, this->role_);
    return 0;
}

int UDPNetworkManager::client_connect_one_rc_qp(uint32_t server_id, 
        __OUT struct MrInfo * mr_info) {
    int rc = 0;
    struct ibv_qp * new_rc_qp = client_create_rc_qp();
    struct KVMsg request;
    memset(&request, 0, sizeof(struct KVMsg));
    request.type = REQ_CONNECT;
    request.id   = server_id_;
    rc = get_qp_info(new_rc_qp, &request.body.conn_info.qp_info);
    serialize_kvmsg(&request);
    rc = sendto(udp_sock_, &request, sizeof(struct KVMsg), 
        0, (struct sockaddr *)&server_addr_list_[server_id], 
        sizeof(struct sockaddr_in));
    struct KVMsg reply;
    int len;
    rc = recvfrom(udp_sock_, &reply, sizeof(struct KVMsg), 
        0, NULL, NULL);
    deserialize_kvmsg(&reply);
    deserialize_kvmsg(&request);
    rc = ib_connect_qp(new_rc_qp, &request.body.conn_info.qp_info, 
        &reply.body.conn_info.qp_info, conn_type_, role_);
    struct MrInfo * new_mr_info = (struct MrInfo *)malloc(sizeof(struct MrInfo));
    memcpy(new_mr_info, &reply.body.conn_info.gc_info, sizeof(struct MrInfo));
    rc_qp_list_[server_id] = new_rc_qp;
    mr_info_list_[server_id] = new_mr_info;
    memcpy(mr_info, &reply.body.conn_info.gc_info, sizeof(struct MrInfo));
    return 0;
}

struct ibv_qp * UDPNetworkManager::client_create_rc_qp() {
    struct ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(struct ibv_qp_init_attr));
    qp_init_attr.qp_type    = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq    = this->ib_cq_;
    qp_init_attr.recv_cq    = this->ib_cq_;
    qp_init_attr.cap.max_send_wr  = 1024;
    qp_init_attr.cap.max_recv_wr  = 1;
    qp_init_attr.cap.max_send_sge = 16;
    qp_init_attr.cap.max_recv_sge = 16;
    return ib_create_rc_qp(this->ib_pd_, &qp_init_attr);
}

struct ibv_qp * UDPNetworkManager::server_create_rc_qp() {
    struct ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(struct ibv_qp_init_attr));
    qp_init_attr.qp_type    = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq    = this->ib_cq_;
    qp_init_attr.recv_cq    = this->ib_cq_;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    qp_init_attr.cap.max_inline_data = 64;
    return ib_create_rc_qp(this->ib_pd_, &qp_init_attr);
}

int UDPNetworkManager::get_qp_info(struct ibv_qp * qp, __OUT struct QpInfo * qp_info) {
    qp_info->qp_num   = qp->qp_num;
    qp_info->lid      = this->ib_port_attr_.lid;
    qp_info->port_num = this->ib_port_num_;
    if (this->conn_type_ == ROCE) {
        memcpy(qp_info->gid, this->ib_gid_, sizeof(union ibv_gid));
    } else {
        memset(qp_info->gid, 0, sizeof(union ibv_gid));
    }
    return 0;
}

void UDPNetworkManager::get_ib_info(__OUT struct IbInfo * ib_info) {
    ib_info->conn_type = this->conn_type_;
    ib_info->ib_ctx    = this->ib_ctx_;
    ib_info->ib_pd     = this->ib_pd_;
    ib_info->ib_cq     = this->ib_cq_;
    ib_info->ib_port_attr = &this->ib_port_attr_;
    ib_info->ib_gid       = this->ib_gid_;
}

int UDPNetworkManager::rdma_post_send_batch_async(uint32_t server_id, struct ibv_send_wr * wr_list) {
    struct ibv_qp * send_qp = this->rc_qp_list_[server_id];
    struct ibv_send_wr * bad_wr;
    return ibv_post_send(send_qp, wr_list, &bad_wr);
}

int UDPNetworkManager::nm_recv_udp_msg(__OUT struct KVMsg * kvmsg, 
        __OUT struct sockaddr_in * src_addr, __OUT socklen_t * src_addr_len) {
    int rc = recvfrom(this->udp_sock_, kvmsg, sizeof(struct KVMsg), 
        0, (struct sockaddr *)src_addr, src_addr_len);
    if (rc != sizeof(struct KVMsg))
        return -1;
    return 0;
}

int UDPNetworkManager::nm_send_udp_msg(struct KVMsg * kvmsg, 
    struct sockaddr_in * dest_addr, socklen_t dest_addr_len) {
    int rc = sendto(this->udp_sock_, kvmsg, sizeof(struct KVMsg), 
        0, (struct sockaddr *)dest_addr, dest_addr_len);
    if (rc != sizeof(struct KVMsg))
        return -1;
    return 0;
}

void UDPNetworkManager::close_udp_sock() {
    close(udp_sock_);
}

inline int UDPNetworkManager::rdma_poll_one_completion(struct ibv_wc * wc) {
    int num_polled = 0;
    while (num_polled == 0) {
        num_polled = ibv_poll_cq(ib_cq_, 1, wc);
    }
    return 0;
}

int UDPNetworkManager::nm_send_udp_msg_to_server(struct KVMsg * kvmsg, uint32_t server_id) {
    struct sockaddr_in * dest_addr = &server_addr_list_[server_id];
    socklen_t dest_addr_len = sizeof(struct sockaddr_in);
    return nm_send_udp_msg(kvmsg, dest_addr, dest_addr_len);
}

int UDPNetworkManager::nm_rdma_write_inl_to_sid(void * data, uint32_t size, uint64_t remote_addr,
        uint32_t remote_rkey, uint32_t server_id) {
    struct ibv_send_wr sr;
    struct ibv_sge     send_sge;
    memset(&send_sge, 0, sizeof(struct ibv_sge));
    send_sge.addr   = (uint64_t)data;
    send_sge.length = size;
    send_sge.lkey   = 0;
    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id = 100;
    sr.sg_list = &send_sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_RDMA_WRITE;
    sr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED | IBV_SEND_FENCE;
    sr.wr.rdma.remote_addr = remote_addr;
    sr.wr.rdma.rkey = remote_rkey;
    int ret = rdma_post_send_batch_async(server_id, &sr);
    struct ibv_wc wc;
    ret = rdma_poll_one_completion(&wc);
    if (wc.status != IBV_WC_SUCCESS) {
        printf("WC status: %d, wr_id: %ld\n", wc.status, wc.wr_id);
        exit(0);
    }
    return 0;
}

int UDPNetworkManager::nm_rdma_write_to_sid(void * local_addr, uint32_t local_lkey, 
        uint32_t size, uint64_t remote_addr, uint32_t remote_rkey, uint32_t server_id) {
    struct ibv_send_wr sr;
    struct ibv_sge     sge;
    memset(&sge, 0, sizeof(struct ibv_sge));
    sge.addr   = (uint64_t)local_addr;
    sge.length = size;
    sge.lkey   = local_lkey;
    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id   = 101;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_RDMA_WRITE;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.rdma.remote_addr = remote_addr;
    sr.wr.rdma.rkey        = remote_rkey;
    int ret = rdma_post_send_batch_async(server_id, &sr);
    struct ibv_wc wc;
    ret = rdma_poll_one_completion(&wc);
    if (wc.status != IBV_WC_SUCCESS) {
        printf("WC status: %d, wr_id: %ld\n", wc.status, wc.wr_id);
        exit(0);
    }
    return 0;
}

int UDPNetworkManager::nm_rdma_read_from_sid(void * local_addr, uint32_t local_lkey,
        uint32_t size, uint64_t remote_addr,
        uint32_t remote_rkey, uint32_t server_id) {
    struct ibv_send_wr sr;
    struct ibv_sge     sge;
    memset(&sge, 0, sizeof(struct ibv_sge));
    sge.addr   = (uint64_t)local_addr;
    sge.length = size;
    sge.lkey   = local_lkey;
    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id = 101;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_RDMA_READ;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.rdma.remote_addr = remote_addr;
    sr.wr.rdma.rkey = remote_rkey;
    int ret = rdma_post_send_batch_async(server_id, &sr);
    struct ibv_wc wc;
    ret = rdma_poll_one_completion(&wc);
    if (wc.status != IBV_WC_SUCCESS) {
        printf("WC status: %d, wr_id: %ld\n", wc.status, wc.wr_id);
        exit(0);
    }
    return 0;
}

int UDPNetworkManager::nm_rdma_cas(void * local_addr, uint32_t local_lkey, 
        uint32_t size, uint64_t remote_addr, uint32_t remote_rkey, uint32_t server_id,
        uint64_t swap_value, uint64_t cmp_value) {
    struct ibv_send_wr sr;
    struct ibv_sge     sge;
    memset(&sge, 0, sizeof(struct ibv_sge));
    sge.addr   = (uint64_t)local_addr;
    sge.length = size;
    sge.lkey   = local_lkey;
    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id   = 101;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.atomic.remote_addr = remote_addr;
    sr.wr.atomic.rkey = remote_rkey;
    sr.wr.atomic.swap = swap_value;
    sr.wr.atomic.compare_add = cmp_value;
    int ret = rdma_post_send_batch_async(server_id, &sr);
    struct ibv_wc wc;
    ret = rdma_poll_one_completion(&wc);
    if (wc.status != IBV_WC_SUCCESS) {
        printf("WC status: %d, wr_id: %ld\n", wc.status, wc.wr_id);
        exit(0);
    }
    return 0;
}

int UDPNetworkManager::nm_rdma_faa(void * local_addr, uint32_t local_lkey, 
        uint32_t size, uint64_t remote_addr, uint32_t remote_rkey, uint32_t server_id,
        uint64_t add_value) {
    struct ibv_send_wr sr;
    struct ibv_sge     sge;
    memset(&sge, 0, sizeof(struct ibv_sge));
    sge.addr   = (uint64_t)local_addr;
    sge.length = size;
    sge.lkey   = local_lkey;
    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id   = 101;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_ATOMIC_FETCH_AND_ADD;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.atomic.remote_addr = remote_addr;
    sr.wr.atomic.rkey = remote_rkey;
    sr.wr.atomic.compare_add = add_value;
    int ret = rdma_post_send_batch_async(server_id, &sr);
    struct ibv_wc wc;
    ret = rdma_poll_one_completion(&wc);
    if (wc.status != IBV_WC_SUCCESS) {
        printf("WC status: %d, wr_id: %ld\n", wc.status, wc.wr_id);
    }
    return 0;
}

int UDPNetworkManager::rdma_post_check_sr_list(map<uint64_t, bool>  *comp_wrid_map){
    int ret;
    int num_wc = comp_wrid_map->size();
    struct ibv_wc * tmp_wc = (struct ibv_wc *)malloc(sizeof(struct ibv_wc) * num_wc);
    do {
        ret = ibv_poll_cq(ib_cq_, num_wc, tmp_wc);
        for (int i = 0; i < ret; i ++) {
            if (tmp_wc[i].status != IBV_WC_SUCCESS) {
                RDMA_LOG(2) << "ret: " << ret << " num_wc: " << num_wc << " wc status(" << tmp_wc[i].status << ")";
                cout << "wc id:" << tmp_wc->wr_id << endl;
                return -1;
            }
            comp_wrid_map[0][tmp_wc[i].wr_id] = true;
        }
    } while (!is_all_complete(*comp_wrid_map));
    return 0;
}

int UDPNetworkManager::rdma_post_sr_list_batch_sync_send(vector<IbvSrList *> & sr_list_batch,
        vector<uint32_t> & sr_list_num_batch, map<uint64_t, bool> & comp_wrid_map) {
    std::map<uint8_t, std::vector<IbvSrList *> > server_id_sr_list_map;
    for (int i = 0; i < sr_list_batch.size(); i ++) {
        uint8_t server_id;
        for (int j = 0; j < sr_list_num_batch[i]; j ++) {
            server_id = sr_list_batch[i][j].server_id;
            server_id_sr_list_map[server_id].push_back(&sr_list_batch[i][j]);
        }
    }
    map<uint8_t, struct ibv_send_wr *> post_sr_map;
    map<uint8_t, std::vector<IbvSrList *> >::iterator it;
    for (it = server_id_sr_list_map.begin(); it != server_id_sr_list_map.end(); it ++) {
        uint64_t last_wr_id;
        struct ibv_send_wr * sr_list_head = ib_merge_sr_lists(it->second, &last_wr_id);
        post_sr_map[it->second[0]->server_id] = sr_list_head;
        if(comp_wrid_map.find(last_wr_id) != comp_wrid_map.end()){
            cout << "wr id: " << last_wr_id << " complicate~";
            exit(0);
        }
        comp_wrid_map[last_wr_id] = false;
    }
    std::map<uint8_t, struct ibv_send_wr *>::iterator sr_it;
    int ret = 0;
    for (sr_it = post_sr_map.begin(); sr_it != post_sr_map.end(); sr_it ++) {
        struct ibv_qp * send_qp = rc_qp_list_[sr_it->first]; 
        struct ibv_send_wr * bad_wr;
        if(!send_qp){
            RDMA_LOG(2) << "send qp is null!" << "server id:" << (uint64_t)sr_it->first;
            exit(0);
        }
        if(!send_qp->context){
            RDMA_LOG(2) << "send qp context is null!" << "server id:" << (uint64_t)sr_it->first;
            exit(0);
        }
        ret = ibv_post_send(send_qp, sr_it->second, &bad_wr);
    }
}

int UDPNetworkManager::rdma_post_sr_list_batch_sync(std::vector<IbvSrList *> & sr_list_batch,
        std::vector<uint32_t> & sr_list_num_batch, __OUT struct ibv_wc * wc) {
    std::map<uint8_t, std::vector<IbvSrList *> > server_id_sr_list_map;
    for (int i = 0; i < sr_list_batch.size(); i ++) {
        uint8_t server_id;
        for (int j = 0; j < sr_list_num_batch[i]; j ++) {
            server_id = sr_list_batch[i][j].server_id;
            server_id_sr_list_map[server_id].push_back(&sr_list_batch[i][j]);
        }
    }
    std::map<uint8_t, struct ibv_send_wr *>  post_sr_map;
    std::map<uint64_t, bool> comp_wrid_map;
    std::map<uint8_t, std::vector<IbvSrList *> >::iterator it;
    for (it = server_id_sr_list_map.begin(); it != server_id_sr_list_map.end(); it ++) {
        uint64_t last_wr_id;
        struct ibv_send_wr * sr_list_head = ib_merge_sr_lists(it->second, &last_wr_id);
        post_sr_map[it->second[0]->server_id] = sr_list_head;
        comp_wrid_map[last_wr_id] = false;
    }
    std::map<uint8_t, struct ibv_send_wr *>::iterator sr_it;
    int ret = 0;
    for (sr_it = post_sr_map.begin(); sr_it != post_sr_map.end(); sr_it ++) {
        struct ibv_qp * send_qp = rc_qp_list_[sr_it->first]; 
        struct ibv_send_wr * bad_wr;
        ret = ibv_post_send(send_qp, sr_it->second, &bad_wr);
    }
    int num_wc = post_sr_map.size();
    struct ibv_wc * tmp_wc = (struct ibv_wc *)malloc(sizeof(struct ibv_wc) * num_wc);
    do {
        ret = ibv_poll_cq(ib_cq_, num_wc, tmp_wc);
        for (int i = 0; i < ret; i ++) {
            if (tmp_wc[i].status != IBV_WC_SUCCESS) {
                RDMA_LOG(2) << "id:" << tmp_wc->wr_id << " wc status(" << tmp_wc[i].status << ")";
                exit(0);
                return -1;
            }
            comp_wrid_map[tmp_wc[i].wr_id] = true;
        }
    } while (!is_all_complete(comp_wrid_map));
    return 0;
}

bool UDPNetworkManager::is_all_complete(const std::map<uint64_t, bool> & wr_id_comp_map) {
    std::map<uint64_t, bool>::const_iterator it;
    // Print status of each wr id for debugging
    for (it = wr_id_comp_map.begin(); it != wr_id_comp_map.end(); it ++) {
        uint64_t wrid = it->first;
        bool done = it->second;
        printf("is_all_complete: wrid=%llu done=%d\n", (unsigned long long)wrid, (int)done);
    }
    fflush(stdout);
    for (it = wr_id_comp_map.begin(); it != wr_id_comp_map.end(); it ++) {
        if (it->second == false) {
            return false;
        }
    }
    return true;
}

int UDPNetworkManager::nm_check_completion(std::map<uint64_t, bool> & wait_wrid_wc_map) {
    std::map<uint64_t, bool>::iterator it;
    printf("nm_check_completion: checking %zu wait wrids\n", wait_wrid_wc_map.size());
    fflush(stdout);
    int found_count = 0;
    for (it = wait_wrid_wc_map.begin(); it != wait_wrid_wc_map.end(); it ++) {
        uint64_t wrid = it->first;
        tbb::concurrent_hash_map<uint64_t, struct ibv_wc *>::const_accessor acc;
        if (wrid_wc_map_.find(acc, wrid)) {
            wait_wrid_wc_map[wrid] = true;
            wrid_wc_map_.erase(acc);
            printf("nm_check_completion: found completion for wrid=%llu\n", (unsigned long long)wrid);
            fflush(stdout);
            found_count++;
        } else {
            printf("nm_check_completion: no completion for wrid=%llu\n", (unsigned long long)wrid);
            fflush(stdout);
        }
    }
    printf("nm_check_completion: found_count=%d\n", found_count);
    fflush(stdout);
    return 0;
}

void UDPNetworkManager::nm_thread_polling() {
    int ret = 0;
    int poll_num = 1024;
    int polled_num = 0;
    int poll_from_server_id;
    struct ibv_wc * wc_buf = (struct ibv_wc *)malloc(sizeof(struct ibv_wc) * poll_num);
    while (stop_polling_ == false) {
        polled_num = ibv_poll_cq(ib_cq_, poll_num, wc_buf);
        for (int i = 0; i < polled_num; i ++) {
            uint64_t wr_id = wc_buf[i].wr_id;
            poll_from_server_id = (wr_id / SERVER_WR_ID) % 10;
            printf("nm_thread_polling: polled wr_id=%llu status=%d poll_from_server_id=%d\n",
                   (unsigned long long)wr_id, wc_buf[i].status, poll_from_server_id);
            fflush(stdout);
            if (wc_buf[i].status != IBV_WC_SUCCESS) {
                printf("%ld client  %ld polled state %d (fb: %ld dst: %ld lwrid: %ld)\n", server_id_, wr_id, wc_buf[i].status, 
                    wr_id / CORO_WR_ID, poll_from_server_id, wr_id % SERVER_WR_ID);
                    exit(0);
            }
            struct ibv_wc * store_wc = (struct ibv_wc *)malloc(sizeof(struct ibv_wc));
            memcpy(store_wc, &wc_buf[i], sizeof(struct ibv_wc));
            tbb::concurrent_hash_map<uint64_t, struct ibv_wc *>::const_accessor acc;
            if (wrid_wc_map_.find(acc, wr_id)) {
                RDMA_LOG(4) << "client:" << server_id_ << " wr_id:" << wr_id << " complication!";
                exit(0);
            }
            tbb::concurrent_hash_map<uint64_t, struct ibv_wc *>::value_type value(wr_id, store_wc);
            wrid_wc_map_.insert(std::move(value));
        }
    }
}

void UDPNetworkManager::stop_polling() {
    stop_polling_ = true;
}

void * nm_polling_thread(void * args) {
    NMPollingThreadArgs * poll_args = (NMPollingThreadArgs *)args;
    UDPNetworkManager * nm = poll_args->nm;
    int ret = 0;
    ret = stick_this_thread_to_core(poll_args->core_id);    
    pthread_t this_tid = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpu_set_t), &cpuset);
    nm->nm_thread_polling();
    return NULL;
}