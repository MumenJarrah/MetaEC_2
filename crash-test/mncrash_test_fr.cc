#include "mncrash_test_fr.h"

WorkloadFileName * get_workload_fname(char * workload_name, int thread_id) {
    
    WorkloadFileName * workload_fname = (WorkloadFileName *)malloc(sizeof(WorkloadFileName));

    sprintf(workload_fname->load_fname, "workloads/%s.spec_load", workload_name);
    sprintf(workload_fname->trans_fname, "workloads/%s.spec_trans%d", workload_name, thread_id);
    // sprintf(workload_fname->load_fname, "upd-workloads/%s.spec_load", workload_name);
    // sprintf(workload_fname->trans_fname, "upd-workloads/%s.spec_trans%d", workload_name, thread_id);

    return workload_fname;
}

int load_workload_mncrash(ClientFR & client, WorkloadFileName * workload_fnames, int st, int ed) {
    int ret = 0;

    client.print_mes("load load~");

    ret = client.load_kv_req_from_file_ycsb(workload_fnames->load_fname, st, ed);
    client.init_kvreq_space(0, 0, client.num_local_operations_);
    for (int i = 0; i < client.num_local_operations_; i ++) {
        client.num_test = i;
        if(i % 500 == 0){
            // printf("test:%d\n", i);
        }
        KVReqCtx * ctx = &client.kv_req_ctx_list_[i];
        // cout << "num test: " << i << " req type: " << ctx->req_type << endl;
        switch (ctx->req_type) {
        case KV_REQ_INSERT:
            ret = client.kv_insert_sync(ctx);
            client.encoding_check();
            // clock_gettime(CLOCK_REALTIME, &et);
            break;
        default:
            break;
        }
    }

    return 0;
}

void timer_fb_func_ms(volatile bool * should_stop, int milliseconds) {

    boost::this_fiber::sleep_for(std::chrono::milliseconds(milliseconds));
    *should_stop = true;

}

int test_workload_mncrash_before(ClientFR & client, WorkloadFileName * workload_fnames, RunClientArgs * args) {

    /*
        All tests are Normal Read
    */
   int ret = 0;

    client.print_mes("load test~");
    ret = client.load_kv_req_from_file_ycsb(workload_fnames->trans_fname, 0, 100000);
    client.print_mes("load test finished~");

    client.test_sync_faa_async();
    client.test_sync_read_async();

    client.print_mes("sync~");

    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    ClientFRFiberArgs * fb_args_list = (ClientFRFiberArgs *)malloc(sizeof(ClientFRFiberArgs) * client.num_coroutines_);
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
    
    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_ops_cont_fr, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    global_barrier.wait();
    boost::fibers::fiber timer_fb;
    if (args->thread_id == 0) {
        pthread_barrier_wait(args->timer_barrier);
        boost::fibers::fiber fb(timer_fb_func_ms, args->should_stop, sleep_ms);
        timer_fb = std::move(fb);
    } else {
        pthread_barrier_wait(args->timer_barrier);
    }
    if (args->thread_id == 0) {
        timer_fb.join();
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

    free(fb_args_list);
    return 0;
}

int test_workload_mncrashed(ClientFR & client, WorkloadFileName * workload_fnames, RunClientArgs * args) {
    
    /*
        Some tests are Degrade Read 
    */
   int ret = 0;

   client.print_mes("load test~");
   ret = client.load_kv_req_file_mncrashed(workload_fnames->trans_fname, 0, 100000);
   client.print_mes("load test finished~");

   client.test_sync_faa_async();
   client.test_sync_read_async_double();
//    client.test_sync_read_async();

   client.print_mes("sync~");

   boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
   ClientFRFiberArgs * fb_args_list = (ClientFRFiberArgs *)malloc(sizeof(ClientFRFiberArgs) * client.num_coroutines_);
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
   *args->should_stop = false;
   
   boost::fibers::fiber fb_list[client.num_coroutines_];
   for (int i = 0; i < client.num_coroutines_; i ++) {
       boost::fibers::fiber fb(client_ops_fb_cnt_ops_cont_fr, &fb_args_list[i]);
       fb_list[i] = std::move(fb);
   }
   global_barrier.wait();
   boost::fibers::fiber timer_fb;
   if (args->thread_id == 0) {
       pthread_barrier_wait(args->timer_barrier);
       boost::fibers::fiber fb(timer_fb_func_ms, args->should_stop, sleep_ms);
       timer_fb = std::move(fb);
   } else {
       pthread_barrier_wait(args->timer_barrier);
   }
   if (args->thread_id == 0) {
       timer_fb.join();
   }

   uint32_t ops_cnt = 0;
   uint32_t num_failed = 0;
   for (int i = 0; i < client.num_coroutines_; i ++) {
       fb_list[i].join();
       ops_cnt += fb_args_list[i].ops_cnt;
       num_failed += fb_args_list[i].num_failed;
   }
   args->ret_num_ops_crashed = ops_cnt;
   args->ret_num_failed_crashed = num_failed;
   free(fb_args_list);
   return 0;
}

void * run_client_mncrash(void * _args) {

    RunClientArgs * args = (RunClientArgs *)_args;

    WorkloadFileName * workload_fnames = get_workload_fname(args->workload_name, args->thread_id + args->client_id);

    int ret = 0;
    GlobalConfig config;
    ret = load_config(args->config_file, &config);
    assert(ret == 0);

    // modify config to config
    config.main_core_id = args->main_core_id;
    config.poll_core_id = args->poll_core_id;
    config.encoding_core_id = args->encoding_core_id;
    config.server_id    += args->thread_id;

    // bind this process to main core
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
        // sleep(1);
    } else if(pre_cn >= 2 && pre_cn < 4){
        sleep(time_range * 1);
    } else if(pre_cn >= 4 && pre_cn < 6){
        sleep(time_range * 2);
    } else if(pre_cn >= 6 && pre_cn < 8){
        sleep(time_range * 3);
    }

    printf("start~\n");

    ClientFR client(&config);

    printf("Client %d start\n", args->thread_id);

    if (args->thread_id == 0 && pre_client_id == 0) {
        client.print_mes("load workload~");
        ret = load_workload_mncrash(client, workload_fnames, 0, -1);
        client.print_mes("load workload finished~");
    } 

    pthread_t polling_tid = client.start_polling_thread();

    client.print_mes("test before mncrashed~");
    ret = test_workload_mncrash_before(client, workload_fnames, args);
    client.print_mes("test before mncrashed finished~");

    client.print_mes("test after mncrashed~");
    ret = test_workload_mncrashed(client, workload_fnames, args);
    client.print_mes("test after mncrashed finished~");

    client.stop_polling_thread();

    pthread_join(polling_tid, NULL);
    
    return 0;
}