#include "fragec/clientfr.h"

void test_op_latency(ClientFR & client, const char * op, int num_op){
    int ret = 0;
    client.load_kv_req_micro_latency(num_op, op);
    uint64_t * lat_list = (uint64_t *)malloc(sizeof(uint64_t) * client.num_local_operations_);
    memset(lat_list, 0, sizeof(uint64_t) * client.num_local_operations_);
    client.num_failed_ = 0;
    void * search_addr;
    void * degrade_read_addr;
    bool should_stop = false;
    client.init_kvreq_space(0, 0, client.num_local_operations_);
    RDMA_LOG(3) << "start test operation: " << op;
    client.init_trace();
    client.init_time_latency(num_op);
    for (int i = 0; i < client.num_local_operations_; i ++) {
        client.num_test = i;
        KVReqCtx * ctx = &client.kv_req_ctx_list_[i];
        ctx->coro_id = 0;
        ctx->should_stop = &should_stop;
        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            search_addr = client.kv_search_sync(ctx);
            if (search_addr == NULL) {
                client.num_failed_ ++;
                client.get_time_latency(true);
            } else {
                client.get_time_latency(false);
                // char value[client.value_size];
                // memcpy(value, search_addr, client.value_size);
                // RDMA_LOG_IF(2, client.if_print_log) << "search value:" << value;
            }
            break;
        case KV_REQ_INSERT:
            ret = client.kv_insert_sync(ctx);
            client.encoding_check_make_trace();
            // clock_gettime(CLOCK_REALTIME, &et);
            break;
        case KV_REQ_UPDATE:
            ret = client.kv_update_sync(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                client.num_failed_ ++;
                client.get_time_latency(true);
            } else {
                client.get_time_latency(false);
            }
            break;
        case KV_REQ_DELETE:
            ret = client.kv_delete_sync(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                client.num_failed_ ++;
                client.get_time_latency(true);
            } else {
                client.get_time_latency(false);
            }
            break;
        case KV_DEGRADE_READ:
            search_addr = client.kv_degread_sync(ctx);
            
            if (search_addr == NULL) {
                client.num_failed_ ++;
                client.get_time_latency(true);
            } else {
                client.get_time_latency(false);
                // RDMA_LOG_IF(2, client.if_print_log) << "degrade read value:";
                // RDMA_LOG_IF(2, client.if_print_log).write((char *)search_addr, client.value_size);
            }
            break;
        default:
            assert(0);
            break;
        }
    }
    client.encoding_wait();
    client.print_time_latency();
    /*
        write file
    */
    client.print_mes("start write latency~");
    char file_name[128];
    sprintf(file_name, "results/op-%s_k-%d_m-%d_FragEC.txt", op, client.k, client.m);
    FILE * lat_fp = fopen(file_name, "w");
    for (int i = 0; i < client.op_id; i ++) {
        fprintf(lat_fp, "%ld\n", client.lat_list[i]);
    }
    fclose(lat_fp);
    client.print_mes("write latency finished~");
}

int main(int argc, char ** argv) {
    if (argc != 4) {
        printf("Usage: %s path-to-config-file num_op op\n", argv[0]);
        return 1;
    }
    int ret = 0;
    GlobalConfig client_config;
    ret = load_config(argv[1], &client_config);
    assert(ret == 0);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(client_config.main_core_id, &cpuset);
    ret = sched_setaffinity(0, sizeof(cpuset), &cpuset);
    assert(ret == 0);
    ret = sched_getaffinity(0, sizeof(cpuset), &cpuset);
    assert(ret == 0);
    // push the main thread to a core
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            RDMA_LOG_IF(3, client_config.if_print_log) << " main process running on core:" << i;
        }
    }
    ClientFR client(&client_config);
    // client.test_read_latency();
    // client.test_write_cas_read();
    // client.test_write_faa_read();
    char *op = argv[3];
    int num_op = atoi(argv[2]);
    test_op_latency(client, "INSERT", num_op);
    client.free_encoding_mm_space();
    client.print_args("make stripe:", client.ectx->stripe_id);
    test_op_latency(client, op, num_op);
    // test_op_latency(client, "SEARCH", num_op);
    // test_op_latency(client, "UPDATE", num_op);
    // test_op_latency(client, "DELETE", num_op);
    return 0;
}