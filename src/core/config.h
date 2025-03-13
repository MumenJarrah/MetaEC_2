#ifndef DDCKV_CONFIG_H_
#define DDCKV_CONFIG_H_

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <arpa/inet.h>
#include <sys/time.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <fstream>
#include <string>
#include <core/logging.hpp>
#define __OUT
using namespace std;

struct BreakDown{
    double **time_list;
    int num_op;
    int num_RTT;
};

enum Role {
    CLIENT,
    SERVER
};

enum ConnType {
    IB,
    ROCE,
};

struct GlobalConfig {
    uint8_t  role;
    uint8_t  conn_type;
    uint32_t server_id;
    uint16_t udp_port;
    uint32_t memory_num;
    char     memory_ips[16][16];
    uint32_t ib_dev_id;
    uint32_t ib_port_id;
    int32_t  ib_gid_idx;
    uint64_t server_base_addr;
    uint64_t server_data_len;
    uint64_t allocate_size;
    uint64_t block_size;
    uint64_t subblock_size;
    uint64_t client_local_size;
    uint32_t num_replication;
    uint32_t num_idx_rep;
    uint32_t num_coroutines;
    uint32_t main_core_id;
    uint32_t poll_core_id;
    uint32_t encoding_core_id;
    uint32_t bg_core_id;
    uint32_t gc_core_id;
    // for master
    uint16_t master_port;
    char     master_ip[16];
    float    miss_rate_threash;
    float    workload_run_time;
    int      micro_workload_num;
    // add
    int key_size;
    int value_size;
    uint32_t client_id;
    int max_stripe;
    int k_data;
    int m_parity;
    int virtual_mn;
    int all_clients;
    bool if_print_log;
    int race_hash_root_size;
    bool is_use_cache;
    int cache_size;
    int num_cn;
    char op[16];
    bool if_load;
    bool if_batch;
    bool is_recovery;
    bool if_req_latency;
};
# endif