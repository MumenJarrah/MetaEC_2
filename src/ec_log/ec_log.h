#ifndef METAEC_EC_LOG_H
#define METAEC_EC_LOG_H

#include<stdint.h>
#include<bitset>

using namespace std;

#define MAX_LOG_ENTRY (1024 * 16)
#define SERVER_LOG_LEN sizeof(ServerLog)
#define LOG_ENTRY_LEN  sizeof(LogEntry)

enum {
    LOG_INSERT_CAS_KV_SLOT,
    LOG_UP_DE_CAS_STRIPE_META,
    LOG_UP_DE_FAA_STRIPE_META,
    LOG_UPDATE_CAS_KV_SLOT,
    LOG_UP_DE_CAS_PL_SLOT,
    LOG_DELETE_OLD_KV_META,
    LOG_DELETE_OLD_KV_SLOT,
    LOG_DEGRADE_CAS_STRIPE_META,
    LOG_DEGRADE_FAA_STRIPE_META,
    LOG_MERGE_MF,
    LOG_MERGE_PARITY_SLOT,
    LOG_MERGE_OLD_PL_SLOT,
    LOG_UPDATE_END,
    LOG_DELETE_END,
    LOG_DEGREAD_END
};

struct Log{
    uint64_t wp;
    uint64_t rp;
    uint64_t remote_wp_addr;
    uint64_t remote_rp_addr;
    uint64_t remote_log_entry_addr;
};

struct LogEntry{
    uint64_t entry_val;
    uint8_t  op_type_uf;
};

struct ClientLog{
    int num_log;
    Log *log_list;
};

struct ServerLog{
    uint64_t client_id;
    uint64_t wp;
    uint64_t rp;
    LogEntry log_entry_list[MAX_LOG_ENTRY];
};

void make_log_entry(LogEntry *log_entry, uint8_t op_type, uint64_t entry_val);
#endif