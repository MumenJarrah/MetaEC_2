#ifndef DDCKV_HASH_RING_H
#define DDCKV_HASH_RING_H

#include "core/hashtable.h"
#include "core/logging.hpp"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <map>

#define MAX_LOAD_HASHRING 3

class SmallHashRing{
public:
    int num_mn;             // num memory node of the smallhashring
    int ring_id;            // the smallhashring's id
    int virtual_mn;
    int mn_id[MAX_LOAD_HASHRING];

    // the map of a memory node's ip and it's position of the smallhashring
    std::map<uint64_t, uint8_t> mn_position; 

    // map mn id to index
    std::map<uint8_t, uint8_t> id_map_index;
public:

    SmallHashRing();
    ~SmallHashRing();

    uint8_t hash_find_id(uint64_t hash_value); // find the mn's ip from a hash_value
    uint8_t hash_find_index(uint64_t hash_value); // find the mn's index of the small hashring from a hash_value

    void hash_find_id_index(uint64_t hash_value, uint8_t & server_id, uint8_t & server_index);
};

class HashRing{

private:
    
    int num_mn;                         // num of memory_node 
    int k_data;                         // num of hash ring storing data
    int m_parity;                       // num of hash ring storing parity
    int num_ring;                       // num of rings
    char memory_ips[16][16];            // memory_ip list

    vector<uint8_t> all_num_mn;

public:

    HashRing(const char * fname, int k_data, int m_parity, int num_mn, int virtual_mn, char memory_ips[16][16]);       
    ~HashRing();                       

    // load config to Hashring
    void load_config(const char * fname); 

    // init small_hashring(there is a way to evenly allocate mn to every hashring)
    void init_small_hash_ring(); 

    // print every mn's message
    void print_map(); 

    // find the mn's ip from a hash_value
    uint8_t hash_map_id(uint64_t hash_value, uint16_t offset); 
    uint8_t hash_map_index(uint64_t hash_value, uint16_t offset);

    // first find hashring by ringid, second map ip by hash value and offset
    uint8_t hash_ringid_map_id(int ringid, uint64_t hash_value, uint16_t offset);
    uint8_t hash_ringid_map_index(int ringid, uint64_t hash_value, uint16_t offset);
    void hash_ringid_map_id_index(int ringid, uint64_t hash_value, uint16_t offset,
        uint8_t & server_id, uint8_t & server_index);
    uint8_t get_ringid(uint64_t hash_value, uint8_t offset);
    vector<uint8_t> get_rep_num_mn(uint64_t hash_value);
    int get_rep_all_combined_mn(uint64_t hash_value);
    int get_ringid_rep_all_combined_mn(uint64_t hash_value, int ringid);
    uint8_t get_num_mn(uint64_t hash_value, uint8_t offset);
    uint8_t get_ringid_num_mn(uint64_t hash_value, uint8_t offset, int ringid);
    vector<uint8_t> *get_all_num_mn();
    SmallHashRing *small_hashring;      // every small_hashring
    int get_k();
    int get_m();
};
#endif