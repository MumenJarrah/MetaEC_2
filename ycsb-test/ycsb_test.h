#ifndef DDCKV_YCSB_TEST_H_
#define DDCKV_YCSB_TEST_H_

#include "compute_pool/client.h"

typedef struct TagWorkloadFileName {
    char load_fname[64];
    char trans_fname[64];
} WorkloadFileName;

typedef struct TagRunClientArgs {
    int thread_id;
    int main_core_id;
    int poll_core_id;
    int encoding_core_id;
    char * workload_name;
    char * config_file;
    pthread_barrier_t * load_barrier;
    volatile bool * should_stop;
    pthread_barrier_t * timer_barrier;
    uint32_t ret_num_ops;
    uint32_t ret_num_failed;
    uint32_t client_id;
    uint32_t num_threads;
    EcMetaCache *ec_meta_cache;
    NumCount code_num_failed;
    char op_type[16];
} RunClientArgs;

WorkloadFileName * get_workload_fname(char * workload_name, int thread_id);
bool time_is_less_than(struct timeval * t1, struct timeval * t2);
int load_workload_1coro(Client & client, WorkloadFileName * workload_fnames, int st, int ed);
int load_test_cnt_ops_mt(Client & client, WorkloadFileName * workload_fnames, RunClientArgs * args);
void timer_fb_func_ms(volatile bool * should_stop, int milliseconds);
void * run_client_tpt(void * _args);
void *client_encoding_fiber(volatile bool * should_stop, Client *client);
#endif