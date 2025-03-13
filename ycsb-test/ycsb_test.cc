#include "ycsb_test.h"


void timer_fb_func_ms(volatile bool * should_stop, int milliseconds) {
    boost::this_fiber::sleep_for(std::chrono::milliseconds(milliseconds));
    *should_stop = true;
}

WorkloadFileName * get_workload_fname(char * workload_name, int thread_id) {
    WorkloadFileName * workload_fname = (WorkloadFileName *)malloc(sizeof(WorkloadFileName));
    sprintf(workload_fname->load_fname, "workloads/%s.spec_load", workload_name);
    sprintf(workload_fname->trans_fname, "workloads/%s.spec_trans%d", workload_name, thread_id);
    return workload_fname;
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

int load_workload_1coro(Client & client, WorkloadFileName * workload_fnames, int st, int ed) {
    int ret = 0;
    client.print_mes("load load~");
    ret = client.load_kv_req_from_file_ycsb(workload_fnames->load_fname, st, ed);
    assert(ret == 0);
    int num_coro = 1;
    bool should_stop = false;
    client.print_mes("start load~");
    client.init_count_server();
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
    boost::fibers::fiber encoding_fb(client_encoding_fiber, &should_stop, &client);
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
    client.print_mes("encoding join~");
    RDMA_LOG_IF(client.if_print_log, 3) << "num stripe:" << client.pre_stripe;
    return 0;
}

int load_test_cnt_ops_mt(Client & client, WorkloadFileName * workload_fnames, RunClientArgs * args) {
    int ret = 0;
    client.print_mes("load test~");
    ret = client.load_kv_req_from_file_ycsb(workload_fnames->trans_fname, 0, 100000);
    client.print_mes("load test finished~");
    client.test_sync_faa_async();
    client.test_sync_read_async();
    client.print_mes("sync~");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = args->should_stop;
        fb_args_list[i].ops_cnt = 0;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;
    int sleep_ms = (int)((float)client.workload_run_time_ * 1000);
    boost::fibers::fiber *encoding_fb;
    // if(strcmp(args->op_type, "workloadd") == 0){
    //     encoding_fb = new boost::fibers::fiber(client_encoding_fiber, args->should_stop, &client);
    // }
    encoding_fb = new boost::fibers::fiber(client_encoding_fiber, args->should_stop, &client);
    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_ops_cont, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }
    global_barrier.wait();
    boost::fibers::fiber timer_fb;
    if (args->thread_id == 0) {
        printf("%d initializes timer\n", args->thread_id);
        pthread_barrier_wait(args->timer_barrier);
        boost::fibers::fiber fb(timer_fb_func_ms, args->should_stop, sleep_ms);
        timer_fb = std::move(fb);
    } else {
        printf("%d wait for timer\n", args->thread_id);
        pthread_barrier_wait(args->timer_barrier);
    }
    printf("%d passed barrier\n", args->thread_id);
    if (args->thread_id == 0) {
        timer_fb.join();
    }
    if(strcmp(args->op_type, "workloadd") == 0){
        encoding_fb->join();
    }
    uint32_t ops_cnt = 0;
    uint32_t num_failed = 0;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
        ops_cnt += fb_args_list[i].ops_cnt;
        num_failed += fb_args_list[i].num_failed;
    }
    args->ret_num_ops = ops_cnt;
    args->ret_num_failed = num_failed;
    args->code_num_failed = client.code_num_count;
    return 0;
}

void * run_client_tpt(void * _args) {
    RunClientArgs * args = (RunClientArgs *)_args;
    WorkloadFileName * workload_fnames = get_workload_fname(args->workload_name, args->thread_id + args->client_id);
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
    assert(ret == 0);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("client %d main process running on core: %d\n", args->thread_id, i);
        }
    }
    int pre_client_id = config.server_id - config.memory_num;
    int pre_cn = pre_client_id / args->num_threads;
    cout << "client:" << pre_client_id << "load file:" << workload_fnames->trans_fname << endl;
    int time_range = 4;
    if(pre_cn >=0 && pre_cn < 2){
    } else if(pre_cn >= 2 && pre_cn < 4){
        sleep(time_range * 1);
    } else if(pre_cn >= 4 && pre_cn < 6){
        sleep(time_range * 2);
    } else if(pre_cn >= 6 && pre_cn < 8){
        sleep(time_range * 3);
    }
    Client client(&config);
    init_num_count(client.code_num_count);
    printf("Client %d start\n", args->thread_id);
    pthread_t polling_tid = client.start_polling_thread();
    if (args->thread_id == 0 && pre_client_id == 0) {
        ret = load_workload_1coro(client, workload_fnames, 0, -1);
    } 
    ret = load_test_cnt_ops_mt(client, workload_fnames, args);
    client.print_mes("test finished~");
    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return 0;
}