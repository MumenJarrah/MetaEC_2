
#include "compute_pool/client.h"

#define MAX_TEST_SIZE 1001

void insert_then_crashed(Client &client){

    int num_test_op = MAX_TEST_SIZE;

    // first insert 1000 req
    client.load_kv_req(num_test_op, "INSERT");
    client.init_kvreq_space(0, 0, num_test_op);
    for(int i = 0;i < num_test_op - 1;i ++){
        client.kv_insert_sync(&client.kv_req_ctx_list_[i]);
        client.encoding_check();
    }

    // insert a req but crashed before RTT3
    client.kv_insert_crash_sync(&client.kv_req_ctx_list_[num_test_op - 1]);

}

void search_after_recovery(Client &client){

    // search 1001 req
    int num_test_op = MAX_TEST_SIZE;

    // first insert 1000 req
    client.load_kv_req(num_test_op, "SEARCH");
    client.init_kvreq_space(0, 0, num_test_op);
    void * search_addr;
    int num_failed = 0;
    int num_finish = 0;
    for(int i = 0;i < num_test_op;i ++){
        search_addr = client.kv_search_sync(&client.kv_req_ctx_list_[i]);
        if(search_addr == NULL){
            num_failed ++;
        } else {
            num_finish ++;
            // char value[client.value_size];
            // memcpy(value, search_addr, client.value_size);
            // RDMA_LOG_IF(2, client.if_print_log) << "search value:" << value;
        }
        
    }

    // cout << "num finished:" << num_finish << endl;
    // cout << "num failed:" << num_failed << endl;

    if(num_finish == num_test_op){
        cout << "recovery complete~" << endl;
    }

}

int main(int argc, char ** argv) {
 
    if (argc != 2) {
        printf("Usage: %s path-to-config-file\n", argv[0]);
    }
    int ret = 0;
    GlobalConfig config;
    ret = load_config(argv[1], &config);

    config.num_coroutines = 1;
    config.is_recovery = false;
    Client client(&config);

    if(config.is_recovery == false){
        // add crashed while insert
        insert_then_crashed(client);
    }

    config.is_recovery = true;
    std::vector<struct timeval> recover_time_bd;
    struct timeval st, et;
    gettimeofday(&st, NULL);
    Client clientr(&config);
    // clientr.print_buffer_all_mes();
    gettimeofday(&et, NULL);
    clientr.get_recover_time(recover_time_bd);
    // test search after recovery
    search_after_recovery(clientr);

    uint64_t connection_recover_time_us = time_spent_us(&recover_time_bd[0], &recover_time_bd[1]);
    uint64_t memory_recover_time_us = time_spent_us(&recover_time_bd[1], &recover_time_bd[2]);
    uint64_t local_mr_reg_time_us = time_spent_us(&recover_time_bd[2], &recover_time_bd[3]);
    uint64_t init_ec_structure_time_us = time_spent_us(&recover_time_bd[3], &recover_time_bd[4]);
    uint64_t get_wp_rp_time_us = time_spent_us(&recover_time_bd[5], &recover_time_bd[6]);
    uint64_t get_unicol_log_time_us = time_spent_us(&recover_time_bd[7], &recover_time_bd[8]);
    uint64_t kv_ops_recover_time_us = time_spent_us(&recover_time_bd[4], &recover_time_bd[5]) + 
        time_spent_us(&recover_time_bd[6], &recover_time_bd[7]) + time_spent_us(&recover_time_bd[8], &recover_time_bd[9]);

    printf("0. connection_recover_time_us: %ld us\n", connection_recover_time_us);
    printf("1. memory_recover_time_us: %ld us\n", memory_recover_time_us);
    printf("2. local_mr_reg_time_us: %ld us\n", local_mr_reg_time_us);
    printf("3. init_ec_structure_time_us: %ld us\n", init_ec_structure_time_us);
    printf("4. get_wp_rp_time_us: %ld us\n", get_wp_rp_time_us);
    printf("5. get_unicol_log_time_us: %ld us\n", get_unicol_log_time_us);
    printf("6. kv_ops_recover_time_us: %ld us\n", kv_ops_recover_time_us);
    printf("total:%ld us\n", time_spent_us(&st, &et));

    return 0;
}