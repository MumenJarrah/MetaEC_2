#include "compute_pool/client.h"

int main(int argc, char ** argv) {
    if (argc != 3) {
        printf("Usage: %s path-to-config-file path-to-test-file\n", argv[0]);
        return 1;
    }
    // load config
    int ret = 0;
    GlobalConfig client_config;
    ret = load_config(argv[1], &client_config);
    assert(ret == 0);
    Client client(&client_config);
    client.load_kv_req_file_twitter(argv[2], 0, -1);
    client.init_kvreq_space(0, 0, client.num_local_operations_);
    client.init_mem_con();
    for (int i = 0; i < client.num_local_operations_; i ++) {
        client.kv_insert_sync(&client.kv_req_ctx_list_[i]);
        client.encoding_check();
    }
    client.mem_con.log = 64 * (8 + 9 * MAX_LOG_ENTRY);
    client.print_mem_con();
    return 0;
}