#include "server.h"

void * server_main(void * server_main_args) {
    ServerMainArgs * args = (ServerMainArgs *)server_main_args;
    Server * server_instance = args->server;
    int ret = stick_this_thread_to_core(args->core_id);
    return server_instance->thread_main();   
}

Server::Server(const struct GlobalConfig * conf) {
    server_id_ = conf->server_id;
    cout << "server id:" << server_id_ << endl;
    need_stop_ = 0;
    if_print_log = conf->if_print_log;
    nm_ = new UDPNetworkManager(conf);
    struct IbInfo ib_info;
    nm_->get_ib_info(&ib_info);
    mm_ = new ServerMM(conf->server_base_addr, conf->server_data_len, conf->allocate_size, &ib_info, conf);
}

Server::~Server() {
    delete nm_;
    delete mm_;
}

int Server::server_on_connect(const struct KVMsg * request, 
    struct sockaddr_in * src_addr, 
    socklen_t src_addr_len) {
    int rc = 0;
    struct KVMsg reply;
    memset(&reply, 0, sizeof(struct KVMsg));
    reply.id   = server_id_;
    reply.type = REP_CONNECT;
    rc = nm_->nm_on_connect_new_qp(request, &reply.body.conn_info.qp_info);
    rc = mm_->get_mr_info(&reply.body.conn_info.gc_info);
    serialize_kvmsg(&reply);
    printf("id: %d, type: %d, addr: %s\n", reply.id, reply.type, inet_ntoa(src_addr->sin_addr));
    rc = nm_->nm_send_udp_msg(&reply, src_addr, src_addr_len);
    deserialize_kvmsg(&reply);
    rc = nm_->nm_on_connect_connect_qp(request->id, &reply.body.conn_info.qp_info, &request->body.conn_info.qp_info);
    return 0;
}

int Server::server_on_alloc(const struct KVMsg * request, struct sockaddr_in * src_addr, 
        socklen_t src_addr_len) {
    uint64_t alloc_addr = mm_->mm_alloc();
    struct KVMsg reply;
    memset(&reply, 0, sizeof(struct KVMsg));
    reply.type = REP_ALLOC;
    reply.id   = nm_->get_server_id();
    reply.body.mr_info.rkey = mm_->get_rkey();
    if (alloc_addr != 0) {
        reply.body.mr_info.addr = alloc_addr; 
    } else {
        printf("server no space\n");
        reply.body.mr_info.addr = 0;
    }
    serialize_kvmsg(&reply);
    int ret = nm_->nm_send_udp_msg(&reply, src_addr, src_addr_len);
    return 0;
}

int Server::server_on_alloc_small_subtable(const struct KVMsg * request, struct sockaddr_in * src_addr,
        socklen_t src_addr_len) {
    uint64_t subtable_addr = mm_->mm_alloc_small_subtable();
    cout << "alloc small subtable addr:" << subtable_addr << endl;
    struct KVMsg reply;
    memset(&reply, 0, sizeof(struct KVMsg));
    reply.type = REP_ALLOC_SMALL_SUBTABLE;
    reply.id   = nm_->get_server_id();
    reply.body.mr_info.addr = subtable_addr; 
    reply.body.mr_info.rkey = mm_->get_rkey();
    serialize_kvmsg(&reply);
    int ret = nm_->nm_send_udp_msg(&reply, src_addr, src_addr_len);
    return 0;
}

int Server::server_on_alloc_big_subtable(const struct KVMsg * request, struct sockaddr_in * src_addr,
        socklen_t src_addr_len) {
    uint64_t subtable_addr = mm_->mm_alloc_big_subtable();
    cout << "alloc big subtable addr:" << subtable_addr << endl;
    struct KVMsg reply;
    memset(&reply, 0, sizeof(struct KVMsg));
    reply.type = REP_ALLOC_BIG_SUBTABLE;
    reply.id   = nm_->get_server_id();
    reply.body.mr_info.addr = subtable_addr; 
    reply.body.mr_info.rkey = mm_->get_rkey();
    serialize_kvmsg(&reply);
    int ret = nm_->nm_send_udp_msg(&reply, src_addr, src_addr_len);
    return 0;
}

int Server::server_on_alloc_log(const struct KVMsg * request, struct sockaddr_in * src_addr,
        socklen_t src_addr_len) {
    uint64_t log_addr = mm_->mm_alloc_log(request->client_id);
    ServerLog *server_log = (ServerLog *)log_addr;
    server_log->client_id = (uint64_t)src_addr;
    struct KVMsg reply;
    memset(&reply, 0, sizeof(struct KVMsg));
    reply.type = REP_ALLOC_LOG;
    reply.id   = nm_->get_server_id();
    reply.body.mr_info.addr = log_addr; 
    reply.body.mr_info.rkey = mm_->get_rkey();
    serialize_kvmsg(&reply);
    int ret = nm_->nm_send_udp_msg(&reply, src_addr, src_addr_len);
    return 0;
}

void * Server::thread_main() {
    struct sockaddr_in client_addr;
    socklen_t          client_addr_len = sizeof(struct sockaddr_in);
    struct KVMsg request;
    int rc = 0;
    while (!need_stop_) {
        rc = nm_->nm_recv_udp_msg(&request, &client_addr, &client_addr_len);
        if (rc && need_stop_) {
            break;
        } else if (rc) {
            continue;
        }
        deserialize_kvmsg(&request);
        if (request.type == REQ_CONNECT) {
            rc = server_on_connect(&request, &client_addr, client_addr_len);
        } 
        else if (request.type == REQ_ALLOC) {
            rc = server_on_alloc(&request, &client_addr, client_addr_len);
        }
        else if (request.type == REQ_ALLOC_SMALL_SUBTABLE) {
            rc = server_on_alloc_small_subtable(&request, &client_addr, client_addr_len);
        } 
        else if (request.type == REQ_ALLOC_BIG_SUBTABLE) {
            rc = server_on_alloc_big_subtable(&request, &client_addr, client_addr_len);
        }
        else if (request.type == REQ_ALLOC_LOG){
            rc = server_on_alloc_log(&request, &client_addr, client_addr_len);
        }
    }
    return NULL;
}

void Server::stop() {
    need_stop_ = 1;
}
