#include "mncrash_test.h"

int main(int argc, char ** argv) {

    if (argc != 3) {
        printf("Usage: %s path-to-config-file workload-name\n", argv[0]);
        return -1;
    }

    GlobalConfig config;
    int ret = load_config(argv[1], &config);
    assert(ret == 0);

    int num_clients = config.all_clients;

    // bind this process to main core
    // run client args
    RunClientArgs * client_args_list = (RunClientArgs *)malloc(sizeof(RunClientArgs) * num_clients);
    pthread_barrier_t global_load_barrier;
    pthread_barrier_init(&global_load_barrier, NULL, num_clients);
    pthread_barrier_t global_timer_barrier;
    pthread_barrier_init(&global_timer_barrier, NULL, num_clients);
    volatile bool should_stop = false;

    pthread_t tid_list[num_clients];
    for (int i = 0; i < num_clients; i ++) {
        client_args_list[i].client_id     = config.server_id - config.memory_num;
        client_args_list[i].thread_id     = i;
        client_args_list[i].main_core_id  = config.main_core_id + i * 3;
        client_args_list[i].poll_core_id  = config.poll_core_id + i * 3;
        client_args_list[i].encoding_core_id  = config.encoding_core_id + i * 3;
        client_args_list[i].config_file   = argv[1];
        client_args_list[i].workload_name = argv[2];
        client_args_list[i].load_barrier  = &global_load_barrier;
        client_args_list[i].should_stop   = &should_stop;
        client_args_list[i].timer_barrier = &global_timer_barrier;
        client_args_list[i].ret_num_ops = 0;
        client_args_list[i].ret_num_failed = 0;
        client_args_list[i].ret_num_ops_crashed = 0;
        client_args_list[i].ret_num_failed_crashed = 0;
        client_args_list[i].num_threads = num_clients;
       
        pthread_t tid;
        pthread_create(&tid, NULL, run_client_mncrash, &client_args_list[i]);
        tid_list[i] = tid;
    }

    uint32_t total_tpt = 0;
    uint32_t total_failed = 0;
    int tpt = 0;
    uint32_t total_tpt_mncrashed = 0;
    uint32_t total_failed_mncrashed = 0;
    int tpt_mncrashed = 0;
    NumCount code_num_failed;
    init_num_count(code_num_failed);
    for (int i = 0; i < num_clients; i ++) {
        pthread_join(tid_list[i], NULL);
        // cout << "debug3" << endl;
        total_tpt += client_args_list[i].ret_num_ops;
        total_failed += client_args_list[i].ret_num_failed;
        total_tpt_mncrashed += client_args_list[i].ret_num_ops_crashed;
        total_failed_mncrashed += client_args_list[i].ret_num_failed_crashed;
        // printf("total: %d ops\n", total_tpt);
        add_num_count(code_num_failed, client_args_list[i].code_num_failed);
    }

    print_num_count(code_num_failed);

    tpt = (total_tpt - total_failed) / config.workload_run_time;
    printf("total op:%d failed:%d tpt:%d\n", total_tpt, total_failed, tpt);

    tpt_mncrashed = (total_tpt_mncrashed - total_failed_mncrashed) / config.workload_run_time;
    printf("total op:%d failed:%d tpt:%d\n", total_tpt_mncrashed, total_failed_mncrashed, tpt_mncrashed);

    char file_name[128];
    FILE *of;

    sprintf(file_name, "./results/MetaEC-clients_%d-thread_%d-mncrashed.txt", 
        config.all_clients, config.server_id);

    printf("filename:%s\n", file_name);

    of = fopen(file_name, "w");
    if(of == NULL){
        cout << "no legal file name~" << endl;
    }
    fprintf(of, "%d %d %d\n", total_tpt, total_failed, tpt);
    fprintf(of, "%d %d %d\n", total_tpt_mncrashed, total_failed_mncrashed, tpt_mncrashed);
    fclose(of);
}