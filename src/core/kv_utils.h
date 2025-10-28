#ifndef DDCKV_KV_UTILS_H
#define DDCKV_KV_UTILS_H

#include <infiniband/verbs.h>
#include <cmath.c>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>
#include <assert.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>
#include <core/logging.hpp>
#include <numeric>
#include <algorithm>

#define __OUT
#define DDCKV_MAX_SERVER 64
#define SERVER_ID_BMASK     0x3F
#define BLOCK_ADDR_BMASK    0x1FFFFFULL
#define BLOCK_OFF_BMASK     0x3FFFFULL
#define SUBBLOCK_NUM_BMASK  0xF
#define MAX_REP_NUM         10 

using namespace std;
#define RACE_HASH_ASSOC_NUM                 (7)

enum KVLogOp {
    KV_OP_INSERT = 1,
    KV_OP_UPDATE,
    KV_OP_DELETE,
    KV_OP_FINISH
};

enum KVMsgType {
    REQ_CONNECT,
    REQ_ALLOC,
    REQ_ALLOC_SMALL_SUBTABLE,
    REQ_ALLOC_BIG_SUBTABLE,
    REQ_ALLOC_LOG,
    REP_CONNECT,
    REP_ALLOC,
    REP_ALLOC_SMALL_SUBTABLE,
    REP_ALLOC_BIG_SUBTABLE,
    REP_ALLOC_LOG,
    REQ_REGISTER,
    REP_REGISTER,
    REQ_RECOVER,
    REP_RECOVER,
    REQ_HEARTBEAT,
    REP_HEAETBEAT
};

typedef struct TagKVInfo {
    void   * l_addr; 
    uint32_t lkey;
    uint32_t key_len;
    uint32_t value_len;
} KVInfo;

typedef struct TagKVRWAddr {
    uint8_t  server_id;
    uint64_t r_kv_addr;
    uint64_t l_kv_addr;
    uint32_t rkey;
    uint32_t lkey;
    uint32_t length;
} KVRWAddr;

typedef struct TagKVCASAddr {
    uint8_t  server_id;
    uint64_t r_kv_addr; 
    uint64_t l_kv_addr; 
    uint32_t rkey;
    uint32_t lkey;
    uint64_t orig_value; 
    uint64_t swap_value; 
} KVCASAddr;

typedef struct TagKVHashInfo {
    uint64_t hash_value; 
    uint64_t prefix;
    uint8_t  fp;
    uint8_t  local_depth;
} KVHashInfo;

typedef struct TagKVTableAddrInfo {
    uint8_t     server_id_list[MAX_REP_NUM];
    uint64_t    f_bucket_addr[MAX_REP_NUM]; 
    uint64_t    s_bucket_addr[MAX_REP_NUM]; 
    uint32_t    f_bucket_addr_rkey[MAX_REP_NUM];
    uint32_t    s_bucket_addr_rkey[MAX_REP_NUM];
    uint32_t    f_main_idx; 
    uint32_t    s_main_idx; 
    uint32_t    f_idx; 
    uint32_t    s_idx; 
} KVTableAddrInfo;

struct QpInfo {
    uint32_t qp_num;
    uint16_t lid;
    uint8_t  port_num;
    uint8_t  gid[16];
    uint8_t  gid_idx;
};

struct MrInfo {
    uint64_t addr;
    uint32_t rkey;
};

struct IbInfo {
    uint8_t conn_type;
    struct ibv_context   * ib_ctx;
    struct ibv_pd        * ib_pd;
    struct ibv_cq        * ib_cq;
    struct ibv_port_attr * ib_port_attr;
    union  ibv_gid       * ib_gid;
};

struct ConnInfo {
    struct QpInfo qp_info;
    struct MrInfo gc_info;
};

struct KVMsg {
    uint16_t  type;
    uint16_t  id;
    uint8_t client_id;
    union {
        struct ConnInfo conn_info;
        struct MrInfo   mr_info;
    } body;
};

static inline uint64_t roundup_256(uint64_t len) {
    if (len % 256 == 0) {
        return len;
    }
    return (len / 256 + 1) * 256;
}

static inline uint64_t time_spent_us(struct timeval * st, struct timeval * et) {
    return (et->tv_sec - st->tv_sec) * 1000000 + (et->tv_usec - st->tv_usec);
}

static inline uint64_t round_up(uint64_t addr, uint32_t align) {
    return ((addr) + align - 1) - ((addr + align - 1) % align);
}

void serialize_kvmsg(__OUT struct KVMsg * kvmsg);
void deserialize_kvmsg(__OUT struct KVMsg * kvmsg);
void serialize_qp_info(__OUT struct QpInfo * qp_info);
void deserialize_qp_info(__OUT struct QpInfo * qp_info);
void serialize_mr_info(__OUT struct MrInfo * mr_info);
void deserialize_mr_info(__OUT struct MrInfo * mr_info);
void serialize_conn_info(__OUT struct ConnInfo * conn_info);
void deserialize_conn_info(__OUT struct ConnInfo * conn_info);
int load_config(const char * fname, __OUT struct GlobalConfig * config);
int stick_this_thread_to_core(int core_id);

// Align the pre_size to the nearest integer at the granularity of sublock_sz_
static inline size_t get_aligned_size(size_t pre_size, size_t subblock_sz_) { 
    if ((pre_size % subblock_sz_) == 0) {
        return pre_size;
    }
    size_t aligned = ((pre_size / subblock_sz_) + 1) * subblock_sz_;
    return aligned;
}

static inline std::vector<int> difference(const std::vector<int>& vec1, const std::vector<int>& vec2) {
    std::vector<int> diff;
    for (int num : vec1) {
        if (std::find(vec2.begin(), vec2.end(), num) == vec2.end()) {
            diff.push_back(num);
        }
    }
    return diff;
}

static inline void convertToBase62(int num, int length, int client_id, const char* delimiter, char* result) {
    const char characters[63] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    int base = 62;
    int index = length - 1;
    for (int i = 0; i < length; ++i) {
        result[i] = '0';
    }
    while (num > 0 && index >= 0) {
        int remainder = num % base;
        result[index] = characters[remainder];
        num /= base;
        index--;
    }
    int delimiter_length = strlen(delimiter);
    for (int i = 0; i < delimiter_length && index >= 0; ++i) {
        result[index] = delimiter[i];
        index--;
    }
    while (client_id > 0 && index >= 0) {
        int remainder = client_id % base;
        result[index] = characters[remainder];
        client_id /= base;
        index--;
    }
    while (index >= 0) {
        result[index--] = '0';
    }
}

static inline void parseBase62(const char* input, int length, int& num, int& client_id, const char* delimiter) {
    const char characters[63] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    int base = 62;
    int index = 0;
    int delimiter_index = -1;
    int delimiter_length = strlen(delimiter);
    for (int i = 0; i < length; ++i) {
        bool match = true;
        for (int j = 0; j < delimiter_length; ++j) {
            if (input[i + j] != delimiter[j]) {
                match = false;
                break;
            }
        }
        if (match) {
            delimiter_index = i;
            break;
        }
    }
    client_id = 0;
    for (int i = delimiter_index - 1; i >= 0; --i) {
        int char_index = 0;
        for (int j = 0; j < 63; ++j) {
            if (input[i] == characters[j]) {
                char_index = j;
                break;
            }
        }
        client_id += char_index * pow(base, delimiter_index - 1 - i);
    }
    num = 0;
    for (int i = length - 1; i > delimiter_index + delimiter_length - 1; --i) {
        int char_index = 0;
        for (int j = 0; j < 63; ++j) {
            if (input[i] == characters[j]) {
                char_index = j;
                break;
            }
        }
        num += char_index * pow(base, length - 1 - i);
    }
}

static inline int compare_desc(const void *a, const void *b, void *arr) {
    int index1 = *(int *)a;
    int index2 = *(int *)b;
    int *array = (int *)arr;
    return (array[index2] - array[index1]);
}

static inline int compare_asc(const void *a, const void *b) {
    return (*(int *)a - *(int *)b);
}

static inline void top_k_indices(int *arr, int m, int k, int *result) {
    int *indices = (int *)malloc(m * sizeof(int));
    for (int i = 0; i < m; i++) {
        indices[i] = i;
    }
    qsort_r(indices, m, sizeof(int), compare_desc, arr);
    for (int i = 0; i < k; i++) {
        result[i] = indices[i];
    }
    qsort(result, k, sizeof(int), compare_asc);
    free(indices);
}

uint32_t read_bits(uint32_t num, int start_bit, int end_bit);
bool write_bits(uint32_t * num, int start_bit, int end_bit, uint32_t value);
uint64_t read_bits(uint64_t num, int start_bit, int end_bit);
bool write_bits(uint64_t * num, int start_bit, int end_bit, uint64_t value);

#endif
