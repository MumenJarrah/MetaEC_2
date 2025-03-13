#ifndef METAEC_MN_CRASH_TEST_H
#define METAEC_MN_CRASH_TEST_H

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
    char * config_file;

    pthread_barrier_t * load_barrier;
    volatile bool * should_stop;
    pthread_barrier_t * timer_barrier;
    char * workload_name;

    uint32_t ret_num_ops;
    uint32_t ret_num_failed;

    uint32_t ret_num_ops_crashed;
    uint32_t ret_num_failed_crashed;

    uint32_t client_id;
    uint32_t num_threads;

    NumCount code_num_failed;

} RunClientArgs;

WorkloadFileName * get_workload_fname(char * workload_name, int thread_id);
void *client_encoding_fiber(volatile bool * should_stop, Client *client);
int load_workload_mncrash(Client & client, WorkloadFileName * workload_fnames, int st, int ed);
void timer_fb_func_ms(volatile bool * should_stop, int milliseconds);
int test_workload_mncrash_before(Client & client, WorkloadFileName * workload_fnames, RunClientArgs * args);
int test_workload_mncrashed(Client & client, WorkloadFileName * workload_fnames, RunClientArgs * args);
void * run_client_mncrash(void * _args);

#endif