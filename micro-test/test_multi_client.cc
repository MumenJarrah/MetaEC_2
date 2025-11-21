#include "test_multi_client.h"

bool time_is_less_than(struct timeval * t1, struct timeval * t2) {
    if (t1->tv_sec < t2->tv_sec) {
        return true;
    } else if (t1->tv_sec > t2->tv_sec) {
        return false;
    }
    if (t1->tv_usec < t2->tv_usec) {
        return true;
    } else if (t1->tv_usec > t2->tv_usec) {
        return false;
    }
    return true;
}

void timer_fb_func_ms(volatile bool * should_stop, int milliseconds) {
    boost::this_fiber::sleep_for(std::chrono::milliseconds(milliseconds));
    *should_stop = true;
}

void *client_encoding_fiber(volatile bool * should_stop, Client *client){
    client->encoding_check_index = 0;
    RDMA_LOG_IF(4, client->if_print_log) << "start encoding fiber!";
    while(*should_stop != true){
        client->encoding_check_async();
        boost::this_fiber::yield();
    }
    while(client->ectx->if_encoding){
        boost::this_fiber::yield();
    };
    client->encoding_leave();
    RDMA_LOG_IF(4, client->if_print_log) << "end encoding fiber!";
}

void *client_encoding_fiber_make_trace(volatile bool * should_stop, Client *client){
    client->init_trace();
    client->encoding_check_index = 0;
    RDMA_LOG_IF(4, client->if_print_log) << "start encoding fiber!";
    while(*should_stop != true){
        client->encoding_check_async_make_trace();
        boost::this_fiber::yield();
    }
    while(client->ectx->if_encoding){
        boost::this_fiber::yield();
    };
    client->encoding_leave_make_trace();
    RDMA_LOG_IF(4, client->if_print_log) << "end encoding fiber!";
}

int load_workload_1coro(Client & client) {
    int ret = 0;
    client.print_mes("load...");
    ret = client.load_kv_req(client.test_num, "INSERT");
    assert(ret == 0);
    int num_coro = client.num_coroutines_;
    volatile bool should_stop = false;
    client.print_mes("Load phase start~");
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * num_coro);
    uint32_t coro_num_ops = client.num_local_operations_ / num_coro;
    for (int i = 0; i < num_coro; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].st = (struct timeval *)malloc(sizeof(struct timeval));
        fb_args_list[i].et = (struct timeval *)malloc(sizeof(struct timeval));
        fb_args_list[i].should_stop = &should_stop;
    }
    fb_args_list[num_coro - 1].ops_num += client.num_local_operations_ % num_coro;
    fb_args_list->should_stop = &should_stop;
    client.check_if_encoding = false;
    // start encoding control fiber
    boost::fibers::fiber encoding_fb(client_encoding_fiber_make_trace, &should_stop, &client);
    boost::fibers::fiber fb_list[num_coro];
    for (int i = 0; i < num_coro; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_time, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }
    for (int i = 0; i < num_coro; i ++) {
        fb_list[i].join();
    }
    should_stop = true;
    encoding_fb.join();
    client.free_encoding_mm_space();
    client.print_mes("encoding join~");
    RDMA_LOG_IF(client.if_print_log, 3) << "num stripe:" << client.pre_stripe;
    return 0;
}

int test_client_tpt_thread(Client & client, RunClientArgs * args) {
    int ret = 0;
    ret = client.load_kv_req(client.test_num, args->op_type);
    if(client.if_req_latency){
        client.client_init_req_latency(MAX_TEST, client.kv_req_ctx_list_[0].req_type);
    }
    client.print_mes("load finish and wait~");
    // client.test_sync_faa_async();
    // client.test_sync_read_async();
    sleep(2);
    client.print_mes("sync!");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    volatile bool should_stop = false;
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = &should_stop;
        fb_args_list[i].ops_cnt = 0;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;
    int sleep_ms = (int)((float)client.workload_run_time_ * 1000);
    boost::fibers::fiber *encoding_fb;
    should_stop = false;
    if(strcmp(args->op_type, "INSERT") == 0){
        encoding_fb = new boost::fibers::fiber(client_encoding_fiber, &should_stop, &client);
    }
    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_ops_cont, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }
    global_barrier.wait();
    boost::fibers::fiber timer_fb(timer_fb_func_ms, &should_stop, sleep_ms);
    timer_fb.join();
    client.print_mes("time tick finished~");
    uint32_t ops_cnt = 0;
    uint32_t num_failed = 0;
    uint64_t tpt = 0;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
        ops_cnt += fb_args_list[i].ops_cnt;
        num_failed += fb_args_list[i].num_failed;
        tpt += fb_args_list[i].tpt;
    }
    if(strcmp(args->op_type, "INSERT") == 0){
        encoding_fb->join();
        client.print_mes("encoding join~");
    }
    args->ret_num_ops = ops_cnt;
    args->ret_num_failed = num_failed;
    args->ret_tpt = tpt;
    tpt = (args->ret_num_ops - args->ret_num_failed) / client.workload_run_time_;
    client.print_args("tpt:", tpt);
    client.print_mes("test client tpt thread finished~");
    if(client.if_req_latency){
        client.client_print_req_latency(args->op_type);
    }
    return 0;
}

void * run_client(void * _args) {
    RunClientArgs * args = (RunClientArgs *)_args;
    int ret = 0;
    GlobalConfig config;
    ret = load_config(args->config_file, &config);
    assert(ret == 0);
    config.main_core_id = args->main_core_id;
    config.poll_core_id = args->poll_core_id;
    config.encoding_core_id = args->encoding_core_id;
    config.server_id    += args->thread_id;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    pthread_t this_tid = pthread_self();
    ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    int pre_client_id = config.server_id - config.memory_num;
    int pre_cn = pre_client_id / args->num_threads;
    int time_range = 4;
    if(pre_cn >=0 && pre_cn < 2){
        // sleep(1);
    } else if(pre_cn >= 2 && pre_cn < 4){
        sleep(time_range * 1);
    } else if(pre_cn >= 4 && pre_cn < 6){
        sleep(time_range * 2);
    } else if(pre_cn >= 6 && pre_cn < 8){
        sleep(time_range * 3);
    }
    Client client(&config);
    pthread_t polling_tid;
    polling_tid = client.start_polling_thread();
    if(strcmp(args->op_type, "DELETE") == 0){
        client.test_num = TEST_NUM * 10;
        client.workload_run_time_ = 1;
    } else {
        client.test_num = TEST_NUM;
    }
    if(strcmp(args->op_type, "INSERT") != 0){
        ret = load_workload_1coro(client);
    }
    if(pre_client_id == 0){
        client.if_req_latency = false;
    }
    ret = test_client_tpt_thread(client, args);
    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return 0;
}

int main(int argc, char ** argv) {
    if (argc != 3) {
        printf("Usage: %s path-to-config-file op\n", argv[0]);
        return 1;
    }
    GlobalConfig config;
    int ret = load_config(argv[1], &config);
    assert(ret == 0);
    int num_clients = config.all_clients;
    char *op = argv[2];
    RunClientArgs * client_args_list = (RunClientArgs *)malloc(sizeof(RunClientArgs) * num_clients);
    pthread_barrier_t global_load_barrier;
    pthread_barrier_init(&global_load_barrier, NULL, num_clients);
    pthread_barrier_t global_timer_barrier;
    pthread_barrier_init(&global_timer_barrier, NULL, num_clients);
    volatile bool should_stop = false;
    pthread_t tid_list[num_clients];
    for (int i = 0; i < num_clients; i ++) {
        client_args_list[i].client_id    = config.server_id - config.memory_num;
        client_args_list[i].thread_id    = i;
        client_args_list[i].main_core_id = config.main_core_id + i * 3;
        client_args_list[i].poll_core_id = config.poll_core_id + i * 3;
        client_args_list[i].encoding_core_id = config.encoding_core_id + i * 3;
        client_args_list[i].config_file   = argv[1];
        client_args_list[i].load_barrier  = &global_load_barrier;
        client_args_list[i].should_stop   = &should_stop;
        client_args_list[i].timer_barrier = &global_timer_barrier;
        client_args_list[i].ret_num_ops = 0;
        client_args_list[i].ret_num_failed = 0;
        client_args_list[i].num_threads = num_clients;
        sprintf(client_args_list[i].op_type, op);
        pthread_t tid;
        pthread_create(&tid, NULL, run_client, &client_args_list[i]);
        tid_list[i] = tid;
    }
    uint32_t total_num_ops = 0;
    uint32_t total_num_failed = 0;
    uint64_t total_tpt = 0;
    for (int i = 0; i < num_clients; i ++) {
        pthread_join(tid_list[i], NULL);
        total_num_ops += client_args_list[i].ret_num_ops;
        total_num_failed += client_args_list[i].ret_num_failed;
        total_tpt += client_args_list[i].ret_tpt;
        RDMA_LOG_IF(3, config.if_print_log) << i << " finished";
    }
    char out_fname[128];
    FILE *of;
    /*
        write tpt
    */
    uint64_t tpt;
    printf("start write write tpt~\n");
    sprintf(out_fname, "results/op_%s-value%d-clients%d-threads%d-tpt-vec.txt", op,
        config.value_size, config.all_clients * config.num_cn, config.server_id);
    of = fopen(out_fname, "w");
    cout << "of:" << of << endl;
    tpt = (double)(total_num_ops - total_num_failed) / float(config.workload_run_time);
    if(strcmp(op, "DELETE") == 0){
        tpt = total_num_ops - total_num_failed;
    }
    printf("op:%s num client:%d total_num_ops:%d total_num_failed:%d tpt:%d workload_run_time:%f\n", 
        op, config.all_clients * config.num_cn, total_num_ops, total_num_failed, tpt, config.workload_run_time);
    fprintf(of, "%d %d %d\n", total_num_ops, total_num_failed, tpt);
    fclose(of);
}