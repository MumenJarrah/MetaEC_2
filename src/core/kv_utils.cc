#include "kv_utils.h"

inline static uint64_t htonll(uint64_t val) {
    return (((uint64_t) htonl(val)) << 32) + htonl(val >> 32);
}
 
inline static uint64_t ntohll(uint64_t val) {
    return (((uint64_t) ntohl(val)) << 32) + ntohl(val >> 32);
}

void serialize_kvmsg(__OUT struct KVMsg * kvmsg) {
    switch (kvmsg->type) {
    case REQ_CONNECT:
    case REP_CONNECT:
        serialize_conn_info(&kvmsg->body.conn_info);
        break;
    case REQ_ALLOC:
    case REP_ALLOC:
    case REQ_ALLOC_SMALL_SUBTABLE:
    case REP_ALLOC_SMALL_SUBTABLE:
        serialize_mr_info(&kvmsg->body.mr_info);
        break;
    default:
        break;
    }
    kvmsg->type = htons(kvmsg->type);
    kvmsg->id   = htons(kvmsg->id);
}

void deserialize_kvmsg(__OUT struct KVMsg * kvmsg) {
    kvmsg->type = ntohs(kvmsg->type);
    kvmsg->id   = ntohs(kvmsg->id);
    switch (kvmsg->type) {
    case REQ_CONNECT:
    case REP_CONNECT:
        deserialize_conn_info(&kvmsg->body.conn_info);
        break;
    case REQ_ALLOC:
    case REP_ALLOC:
    case REQ_ALLOC_SMALL_SUBTABLE:
    case REP_ALLOC_SMALL_SUBTABLE:
        deserialize_mr_info(&kvmsg->body.mr_info);
        break;
    default:
        break;
    }
}

void serialize_qp_info(__OUT struct QpInfo * qp_info) {
    qp_info->qp_num = htonl(qp_info->qp_num);
    qp_info->lid    = htons(qp_info->lid);
}

void deserialize_qp_info(__OUT struct QpInfo * qp_info) {
    qp_info->qp_num = ntohl(qp_info->qp_num);
    qp_info->lid    = ntohs(qp_info->lid);
}

void serialize_mr_info(__OUT struct MrInfo * mr_info) {
    mr_info->addr = htonll(mr_info->addr);
    mr_info->rkey = htonl(mr_info->rkey);
}

void deserialize_mr_info(__OUT struct MrInfo * mr_info) {
    mr_info->addr = ntohll(mr_info->addr);
    mr_info->rkey = ntohl(mr_info->rkey);
}

void serialize_conn_info(__OUT struct ConnInfo * conn_info) {
    serialize_qp_info(&conn_info->qp_info);
    serialize_mr_info(&conn_info->gc_info);
}

void deserialize_conn_info(__OUT struct ConnInfo * conn_info) {
    deserialize_qp_info(&conn_info->qp_info);
    deserialize_mr_info(&conn_info->gc_info);
}

int stick_this_thread_to_core(int core_id) {
    int num_cores = sysconf(_SC_NPROCESSORS_CONF);
    if (core_id < 0 || core_id >= num_cores) {
        return -1;
    }
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_t current_thread = pthread_self();
    return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

uint32_t read_bits(uint32_t num, int start_bit, int end_bit) {
    if (start_bit < 0 || start_bit > 31 || end_bit < 0 || end_bit > 31 || start_bit > end_bit) {
        std::cerr << "Invalid start_bit or end_bit\n";
        return 0;
    }
    uint32_t mask = ((1 << (end_bit - start_bit + 1)) - 1) << start_bit;
    return (num & mask) >> start_bit;
}

bool write_bits(uint32_t * num, int start_bit, int end_bit, uint32_t value) {
    if (start_bit < 0 || start_bit > 31 || end_bit < 0 || end_bit > 31 || start_bit > end_bit) {
        std::cerr << "Invalid start_bit or end_bit\n";
        return false;
    }
    value &= (1 << (end_bit - start_bit + 1)) - 1;
    uint32_t clear_mask = ~(((1 << (end_bit - start_bit + 1)) - 1) << start_bit);
    *num &= clear_mask;
    *num |= value << start_bit;
    return true;
}

uint64_t read_bits(uint64_t num, int start_bit, int end_bit) {
    if (start_bit < 0 || start_bit > 63 || end_bit < 0 || end_bit > 63 || start_bit > end_bit) {
        std::cerr << "Invalid start_bit or end_bit\n";
        return 0;
    }
    uint64_t mask = ((1ULL << (end_bit - start_bit + 1)) - 1) << start_bit;
    return (num & mask) >> start_bit;
}

bool write_bits(uint64_t *num, int start_bit, int end_bit, uint64_t value) {
    if (start_bit < 0 || start_bit > 63 || end_bit < 0 || end_bit > 63 || start_bit > end_bit) {
        std::cerr << "Invalid start_bit or end_bit\n";
        return false;
    }
    value &= (1ULL << (end_bit - start_bit + 1)) - 1;
    uint64_t clear_mask = ~(((1ULL << (end_bit - start_bit + 1)) - 1) << start_bit);
    *num &= clear_mask;
    *num |= value << start_bit;
    return true;
}