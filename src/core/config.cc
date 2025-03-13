#include "config.h"
using namespace std;

int load_config(const char * fname, __OUT struct GlobalConfig * config) {

    std::fstream config_fs(fname);
    boost::property_tree::ptree pt;
    try {
        boost::property_tree::read_json(config_fs, pt);
    } catch (boost::property_tree::ptree_error & e) {
        return -1;
    }

    try {
        std::string role_str = pt.get<std::string>("role");
        if (role_str == std::string("SERVER")) {
            config->role = SERVER;
        } else {
            config->role = CLIENT;
        }
        std::string conn_type_str = pt.get<std::string>("conn_type");
        if (conn_type_str == std::string("IB")) {
            config->conn_type = IB;
        } else {
            config->conn_type = ROCE;
        }
        config->server_id = pt.get<uint32_t>("server_id");
        config->udp_port  = pt.get<uint16_t>("udp_port");
        config->memory_num = pt.get<uint16_t>("memory_num");
        int i = 0;
        BOOST_FOREACH(boost::property_tree::ptree::value_type & v, pt.get_child("memory_ips")) {
            std::string ip = v.second.get<std::string>("");
            strcpy(config->memory_ips[i], ip.c_str());
            i ++;
        }
        config->ib_dev_id = pt.get<uint32_t>("ib_dev_id");
        config->ib_port_id = pt.get<uint32_t>("ib_port_id");
        config->ib_gid_idx = pt.get<uint32_t>("ib_gid_idx", -1);
        std::string server_base_addr_str = pt.get<std::string>("server_base_addr");
        sscanf(server_base_addr_str.c_str(), "0x%lx", &config->server_base_addr);
        config->server_data_len   = pt.get<uint64_t>("server_data_len");
        config->allocate_size        = pt.get<uint64_t>("allocate_size");
        using namespace std;
        config->block_size        = pt.get<uint64_t>("block_size");
        config->subblock_size     = pt.get<uint64_t>("subblock_size");
        config->client_local_size = pt.get<uint64_t>("client_local_size");
        config->num_replication   = pt.get<uint32_t>("num_replication", 1);
        config->num_coroutines    = pt.get<uint32_t>("num_coroutines", 1);
        config->main_core_id = pt.get<uint32_t>("main_core_id", 0);
        config->poll_core_id = pt.get<uint32_t>("poll_core_id", 0);
        config->encoding_core_id = pt.get<uint32_t>("encoding_core_id", 0);
        config->bg_core_id   = pt.get<uint32_t>("bg_core_id", 0);
        config->gc_core_id   = pt.get<uint32_t>("gc_core_id", 0);
        config->num_idx_rep  = pt.get<uint32_t>("num_idx_rep", 1);
        config->miss_rate_threash = pt.get<float>("miss_rate_threash", 0.1);
        config->workload_run_time = pt.get<float>("workload_run_time", 1.0);
        config->micro_workload_num = pt.get<int>("micro_workload_num", 10000);
        config->value_size = pt.get<int>("value_size", 1024);
        config->key_size = pt.get<int>("key_size", 1024);
        config->client_id = pt.get<uint32_t>("client_id", 0);
        config->max_stripe = pt.get<uint32_t>("max_stripe", 10000);
        config->k_data = pt.get<uint32_t>("k_data", 4);
        config->m_parity = pt.get<uint32_t>("m_parity", 2);
        config->virtual_mn = pt.get<uint32_t>("virtual_mn", 3);
        config->all_clients = pt.get<uint32_t>("all_clients", 1);
        config->if_print_log = pt.get<bool>("if_print_log", 1);
        config->race_hash_root_size = pt.get<int>("race_hash_root_size", 6);
        config->cache_size = pt.get<int>("cache_size", 500);
        config->is_use_cache = pt.get<bool>("is_use_cache", false);
        config->num_cn = pt.get<int>("num_cn", 8);
        config->if_load = pt.get<bool>("if_load", true);
        config->if_batch = pt.get<bool>("if_batch", true);
        config->is_recovery = pt.get<bool>("is_recovery", false);
        config->if_req_latency = pt.get<bool>("if_req_latency", false);
    } catch (boost::property_tree::ptree_error & e) {
        cout<<"load config failed!\n";
        return -1;
    }
    return 0;
}
