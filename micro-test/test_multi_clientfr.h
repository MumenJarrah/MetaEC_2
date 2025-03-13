#ifndef DDCKV_TEST_MULTI_CLIENTFR_H
#define DDCKV_TEST_MULTI_CLIENTFR_H

#include "fragec/clientfr.h"
#define TEST_NUM 10000

enum {
    RUN_POLL_CQ_THREAD,
    RUN_POLL_CQ_FIBER,
    RUN_POLL_CQ_CORO,
    RUN_ONLY_RDMA,
    RUN_ONLY_RDMA_TIMER
};

typedef struct TagWorkloadFileName {
    char load_fname[64];
    char trans_fname[64];
} WorkloadFileName;

typedef struct TagRunClientArgs {
    ClientFR *client;
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
    uint64_t ret_tpt;
    uint32_t client_id;
    uint32_t num_threads;
    char op_type[16];
    int url_len;
    int num_test;
} RunClientFRArgs;

void * run_client(void * _args);
int test_client_tpt_thread(ClientFR & client, RunClientFRArgs * args);

#endif