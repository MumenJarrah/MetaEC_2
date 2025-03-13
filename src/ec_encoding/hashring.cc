#include "hashring.h"

HashRing::HashRing(const char * fname, int k_data, int m_parity, int num_mn, int virtual_mn, char memory_ips[16][16]){
    if(fname){
        RDMA_LOG(3) << "load hasring config...";
        load_config(fname);
    }
    else {
        this->k_data = k_data;
        this->m_parity = m_parity;
        this->num_mn = num_mn;
        this->num_ring = k_data + m_parity;
        small_hashring = new SmallHashRing[num_ring];
        int i = 0;
        for(i = 0;i < num_ring;i ++){
            small_hashring[i].virtual_mn = virtual_mn;
        }
        for(i = 0;i < 16;i ++){
            for(int j = 0; j < 16;j ++){
                this->memory_ips[i][j] = memory_ips[i][j];
            }
        }
    }
    init_small_hash_ring();
    for(int i = 0;i < num_ring;i ++){
        all_num_mn.push_back(small_hashring[i].num_mn);
    }
}

HashRing::~HashRing(){
    delete [] small_hashring;
    printf("delete Hashring!\n");
}

void HashRing::load_config(const char * fname){
    std::fstream config_fs(fname);
    boost::property_tree::ptree pt;
    try {
        boost::property_tree::read_json(config_fs, pt);
    } catch (boost::property_tree::ptree_error & e) {
        printf("read json error!\n");
    }
    try {
        this->k_data = pt.get<std::uint32_t>("k_data");
        this->m_parity = pt.get<std::uint32_t>("m_parity");
        this->num_mn = pt.get<std::uint32_t>("memory_node");
        this->num_ring = k_data + m_parity;
        this->small_hashring = new SmallHashRing[num_ring];
        int i = 0;
        for(i = 0;i < num_mn;i ++){
            small_hashring[i].virtual_mn = pt.get<std::uint32_t>("virtual_mn");
        }
        i = 0;
        BOOST_FOREACH(boost::property_tree::ptree::value_type & v, pt.get_child("memory_ips")) {
            std::string ip = v.second.get<std::string>("");
            strcpy(memory_ips[i], ip.c_str());
            i ++;
        }
    } catch (boost::property_tree::ptree_error & e) {
        printf("load error!\n");
    }
}

void HashRing::init_small_hash_ring(){
    int mn_per_ring_max = (num_mn % num_ring) ? (num_mn / num_ring + 1) : (num_mn / num_ring);
    int leave_mn = num_mn;
    int leave_ring = num_ring;
    uint8_t pre_mn = 0;
    bool mod0 = false;
    uint64_t position;
    for(int i = 0;i < num_ring;i ++){
        small_hashring[i].ring_id = i;
        if((!(leave_mn % leave_ring)) && i == 0){
            mod0 = true;
        }
        if(mod0){
            small_hashring[i].num_mn = mn_per_ring_max;
        }
        else {
            if (leave_mn % leave_ring){
                small_hashring[i].num_mn = mn_per_ring_max;
            }
            else {
                small_hashring[i].num_mn = mn_per_ring_max - 1;
            }
        }
        leave_mn -= small_hashring[i].num_mn;
        leave_ring --;
        uint64_t range;
        if(small_hashring[i].num_mn == 1){
            range = 0;
        } else {
            range = (pow(2, 63) - 1) / small_hashring[i].num_mn;
            range = range * 2;
        }
        for(int j = 0;j < small_hashring[i].num_mn; j ++){
            if(small_hashring[i].num_mn == 1){
                position = UINT64_MAX;
            } else {
                position = 1;
            }
            small_hashring[i].mn_position.insert(std::pair<uint64_t, uint8_t>(
                position + j * range,
                pre_mn)
            );
            small_hashring[i].mn_id[j] = pre_mn;
            small_hashring[i].id_map_index[pre_mn] = j;
            pre_mn ++;
        }
    }
}

void HashRing::print_map(){
    RDMA_LOG(3) << "print every mn message";
    for(int i = 0;i < num_ring;i ++){
        std::map<uint64_t, uint8_t>::iterator it;
        for(it = small_hashring[i].mn_position.begin(); it != small_hashring[i].mn_position.end(); it++){
            RDMA_LOG(3) << "ring: " << i << "ip: " << (uint16_t)it->second << " position: " << it->first;
        }
    }
}

uint8_t HashRing::get_num_mn(uint64_t hash_value, uint8_t offset){
    return small_hashring[(hash_value % num_ring + offset) % num_ring].num_mn;
}

uint8_t HashRing::get_ringid_num_mn(uint64_t hash_value, uint8_t offset, int ringid){
    return small_hashring[(ringid + offset) % num_ring].num_mn;
}

vector<uint8_t> HashRing::get_rep_num_mn(uint64_t hash_value){
    vector<uint8_t> ret;
    for(int i = 1;i < m_parity + 1;i ++){
        ret.push_back(get_num_mn(hash_value, i));
    }
    return ret;
}

vector<uint8_t> *HashRing::get_all_num_mn(){
    return &all_num_mn;
}

int HashRing::get_rep_all_combined_mn(uint64_t hash_value){
    int ret = 1;
    for(int i = 1;i < m_parity + 1;i ++){
        ret *= get_num_mn(hash_value, i);
    }
    return ret;
}

int HashRing::get_ringid_rep_all_combined_mn(uint64_t hash_value, int ringid){
    int ret = 1;
    for(int i = 1;i < m_parity + 1;i ++){
        ret *= get_ringid_num_mn(hash_value, i, ringid);
    }
    return ret;
}

uint8_t HashRing::get_ringid(uint64_t hash_value, uint8_t offset){
    return (hash_value % num_ring + offset) % num_ring;
}

uint8_t HashRing::hash_map_id(uint64_t hash_value, uint16_t offset = 0){
    return small_hashring[(hash_value % num_ring + offset) % num_ring].hash_find_id(hash_value);
};

uint8_t HashRing::hash_map_index(uint64_t hash_value, uint16_t offset = 0){
    return small_hashring[(hash_value % num_ring + offset) % num_ring].hash_find_index(hash_value);
};

uint8_t HashRing::hash_ringid_map_id(int ringid, uint64_t hash_value, uint16_t offset){
    return small_hashring[(ringid + offset) % num_ring].hash_find_id(hash_value);
}

uint8_t HashRing::hash_ringid_map_index(int ringid, uint64_t hash_value, uint16_t offset){
    return small_hashring[(ringid + offset) % num_ring].hash_find_index(hash_value);
}

void HashRing::hash_ringid_map_id_index(int ringid, uint64_t hash_value, uint16_t offset,
    uint8_t & server_id, uint8_t & server_index){
    return small_hashring[(ringid + offset) % num_ring].hash_find_id_index(hash_value,
        server_id, server_index);
}

SmallHashRing::SmallHashRing(){
    "nothing";
}

SmallHashRing::~SmallHashRing(){
    "nothing";
}

uint8_t SmallHashRing::hash_find_id(uint64_t hash_value){
    uint8_t ret_value;
    bool found = false;
    int count = 0;
    std::map<uint64_t, uint8_t>::iterator it;
    for(it = mn_position.begin(); it != mn_position.end(); it++){
        if(hash_value < it->first){
            found = true;
            ret_value = it->second;
            break;
        }
        else {
            if(count == num_mn * virtual_mn - 1){
                ret_value = mn_position.begin()->second;
                break;
            }
        }
        count ++;
    }
    return ret_value;
}

uint8_t SmallHashRing::hash_find_index(uint64_t hash_value){
    uint8_t ret_value;
    bool found = false;
    int count = 0;
    std::map<uint64_t, uint8_t>::iterator it;
    for(it = mn_position.begin(); it != mn_position.end(); it++){
        if(hash_value < it->first){
            found = true;
            ret_value = it->second;
            break;
        }
        else {
            if(count == num_mn * virtual_mn - 1){
                ret_value = mn_position.begin()->second;
                break;
            }
        }
        count ++;
    }
    return id_map_index[ret_value];
}

void SmallHashRing::hash_find_id_index(uint64_t hash_value, uint8_t & server_id, uint8_t & server_index){
    uint8_t ret_value;
    bool found = false;
    int count = 0;
    std::map<uint64_t, uint8_t>::iterator it;
    for(it = mn_position.begin(); it != mn_position.end(); it++){
        if(hash_value < it->first){
            found = true;
            ret_value = it->second;
            break;
        }
        else {
            if(count == num_mn * virtual_mn - 1){
                ret_value = mn_position.begin()->second;
                break;
            }
        }
        count ++;
    }
    server_id = ret_value;
    server_index = id_map_index[ret_value];
}

int HashRing::get_k(){
    return k_data;
}

int HashRing::get_m(){
    return m_parity;
}