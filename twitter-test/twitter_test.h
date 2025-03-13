#ifndef METAEC_TWITTER_TEST_H
#define METAEC_TWITTER_TEST_H

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
    WorkloadFileName * workload_fnames;
    uint32_t ret_num_ops;
    uint32_t ret_num_failed;
    uint32_t client_id;
    uint32_t num_threads;
    EcMetaCache *ec_meta_cache;
    NumCount code_num_failed;
    int cluster_id;
} RunClientArgs;

WorkloadFileName * get_workload_fname(int cluster_id, int thread_id);
void *client_encoding_fiber(volatile bool * should_stop, Client *client);
int load_workload(Client & client, WorkloadFileName * workload_fnames, int st, int ed);
void timer_fb_func_ms(volatile bool * should_stop, int milliseconds);
int test_workload(Client & client, WorkloadFileName * workload_fnames, RunClientArgs * args);
void * run_client_twitter(void * _args);

#endif