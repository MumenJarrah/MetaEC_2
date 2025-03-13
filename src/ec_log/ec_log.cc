#include "ec_log.h"

void make_log_entry(LogEntry *log_entry, uint8_t op_type, uint64_t entry_val){
    log_entry->entry_val = entry_val;
    log_entry->op_type_uf = (op_type << 1) + 1;
}