#include "compute_pool/client.h"

void test_op_latency(Client & client, const char * op, int num_op){
    int ret = 0;
    client.load_kv_req_micro_latency(num_op, op);
    uint64_t * lat_list = (uint64_t *)malloc(sizeof(uint64_t) * client.num_local_operations_);
    memset(lat_list, 0, sizeof(uint64_t) * client.num_local_operations_);
    uint32_t num_failed = 0;
    void * search_addr;
    void * degrade_read_addr;
    struct timespec st, et, st1, et1;
    bool should_stop = false;
    client.init_kvreq_space(0, 0, client.num_local_operations_);
    double total_latency = 0;
    RDMA_LOG(3) << "start test operation: " << op;
    client.init_trace();
    for (int i = 0; i < client.num_local_operations_; i ++) {
        client.num_test = i;
        KVReqCtx * ctx = &client.kv_req_ctx_list_[i];
        ctx->coro_id = 0;
        ctx->should_stop = &should_stop;
        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            clock_gettime(CLOCK_REALTIME, &st);
            search_addr = client.kv_search_sync_batch(ctx);
            clock_gettime(CLOCK_REALTIME, &et);
            if (search_addr == NULL) {
                num_failed ++;
            } else {
                // char value[client.value_size];
                // memcpy(value, search_addr, client.value_size);
                // RDMA_LOG_IF(2, client.if_print_log) << "search value:" << value;
            }
            break;
        case KV_REQ_INSERT:
            clock_gettime(CLOCK_REALTIME, &st);
            ret = client.kv_insert_sync_batch(ctx);
            clock_gettime(CLOCK_REALTIME, &et);
            client.encoding_check_make_trace();
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            break;
        case KV_REQ_UPDATE:
            clock_gettime(CLOCK_REALTIME, &st);
            ret = client.kv_update_sync(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            clock_gettime(CLOCK_REALTIME, &et);
            break;
        case KV_REQ_DELETE:
            clock_gettime(CLOCK_REALTIME, &st);
            ret = client.kv_delete_sync(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            clock_gettime(CLOCK_REALTIME, &et);
            break;
        case KV_DEGRADE_READ:
            clock_gettime(CLOCK_REALTIME, &st);
            search_addr = client.kv_degread_sync(ctx);
            // for degrade read breakdown
            // search_addr = client.kv_degread_bd(ctx);
            clock_gettime(CLOCK_REALTIME, &et);
            if (search_addr == NULL) {
                num_failed ++;
            } else {
                // RDMA_LOG_IF(2, client.if_print_log) << "degrade read value:";
                // RDMA_LOG_IF(2, client.if_print_log).write((char *)search_addr, client.value_size);
            }
            break;
        default:
            assert(0);
            break;
        }
        lat_list[i] = (double)((et.tv_sec - st.tv_sec) * 1000000000 + (et.tv_nsec - st.tv_nsec)) / 1000.0;
        total_latency += (double)lat_list[i];
    }
    client.encoding_wait();
    client.print_args("num failed:", num_failed);
    client.print_args("total latency:", total_latency);
    client.print_args("num operation:", client.num_local_operations_);
    double ave_lat = (double)total_latency / (double)client.num_local_operations_;
    client.print_args("average latency(us):", ave_lat);
    /*
        write file
    */
    client.print_mes("start write latency~");
    char file_name[128];
    sprintf(file_name, "results/op-%s_k-%d_m-%d_MetaEC_nolog.txt", op, client.k, client.m);
    FILE * lat_fp = fopen(file_name, "w");
    for (int i = 0; i < client.num_local_operations_; i ++) {
        fprintf(lat_fp, "%ld\n", lat_list[i]);
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
    Client client(&client_config);
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