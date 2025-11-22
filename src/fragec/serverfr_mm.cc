#include "serverfr_mm.h"

#include <unistd.h>
#include <sys/mman.h>
#include <assert.h>
#include <ec_log/ec_log.h>

#define MAP_HUGE_2MB        (21 << MAP_HUGE_SHIFT)
#define MAP_HUGE_1GB        (30 << MAP_HUGE_SHIFT)

ServerFRMM::ServerFRMM(uint64_t server_base_addr, uint64_t base_len, 
    uint32_t block_size, const struct IbInfo * ib_info,
    const struct GlobalConfig * conf) {
    if_print_log = 1;
    allocate_size = block_size;
    base_addr_ = server_base_addr;
    base_len_  = base_len;
    int port_flag = PROT_READ | PROT_WRITE;
    int mm_flag   = MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_HUGETLB | MAP_HUGE_2MB;
    data_ = mmap((void *)this->base_addr_, this->base_len_, port_flag, mm_flag, -1, 0);
    if (data_ == MAP_FAILED) {
        printf("ERROR: mmap failed for address 0x%lx with size 0x%lx\n", this->base_addr_, this->base_len_);
        printf("Retrying without MAP_FIXED flag...\n");
        data_ = mmap(NULL, this->base_len_, port_flag, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB, -1, 0);
        if (data_ == MAP_FAILED) {
            printf("ERROR: mmap failed even without MAP_FIXED\n");
            exit(1);
        }
        this->base_addr_ = (uint64_t)data_;
        printf("New base_addr: 0x%lx\n", this->base_addr_);
    }
    client_meta_area_off_ = 0;
    client_meta_area_len_ = META_AREA_LEN;
    client_gc_area_off_ = this->client_meta_area_len_;
    client_gc_area_len_ = GC_AREA_LEN;
    client_hash_area_off_ = this->client_gc_area_off_ + this->client_gc_area_len_;

    // print hashtable size
    print_hashtable_size();

    //init hash index
    init_hashtable();

    client_log_area_off_ = big_subtable_addr + max_big_subtable * roundup_256(BIG_SUBTABLE_LEN);
    client_hash_area_len_ = client_log_area_off_ - kv_ec_meta_root_addr;

    cout << "all hash area len:" << client_hash_area_len_ << endl;

    client_log_area_len_ = LOG_AREA_LEN;
    client_kv_area_off_ = this->client_log_area_off_ + this->client_log_area_len_;
    client_kv_area_off_ = round_up(client_kv_area_off_, allocate_size);
    client_kv_area_len_ = base_len_ - client_kv_area_off_;
    client_kv_area_limit_ = base_len_ + base_addr_;
    int access_flag = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | 
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

    mr_ = ibv_reg_mr(ib_info->ib_pd, data_, base_len_, access_flag);
    num_memory_ = conf->memory_num;
    num_replication_ = conf->num_replication;
    my_sid_ = conf->server_id;
    printf("my_sid_: %d, num_memory_: %d\n", my_sid_, num_memory_);

    // init blocks
    num_blocks_ = client_kv_area_len_ / allocate_size;
    get_allocable_blocks(); 

}

ServerFRMM::~ServerFRMM() {
    munmap(data_, this->base_len_);
}

void ServerFRMM::print_hashtable_size(){
    cout << "print hashtable size~" << endl;
    cout << "kv ec meta root table:" << ROOT_KV_META_LEN << endl;
    cout << "stripe meta root table:" << ROOT_STRIPE_META_LEN << endl;
    cout << "PL root table:" << ROOT_PL_LEN << endl;
    cout << "metadata root table:" << ROOT_METADATA_LEN << endl;
    cout << "print finished~" << endl;
}

void ServerFRMM::get_allocable_blocks() { 
    uint64_t kv_area_addr = base_addr_ + client_kv_area_off_;
    std::vector<uint64_t> mn_addr_ptr;
    for (int i = 0; i < num_memory_; i ++)
        mn_addr_ptr.push_back(kv_area_addr);
    uint32_t num_rep_blocks = (num_blocks_ * num_memory_) / num_replication_;
    printf("num_rep_blocks: %d, num_blocks: %d\n", 
        num_rep_blocks, num_blocks_);
    uint32_t block_cnt = 0;
    while (block_cnt < num_rep_blocks) {
        uint32_t st_sid = block_cnt % num_memory_;
        while (mn_addr_ptr[st_sid] == client_kv_area_limit_)
            st_sid = (st_sid + 1) % num_memory_;
        uint64_t addr_list[num_replication_];
        for (int i = 0; i < num_replication_; i ++) {
            uint8_t sid = (st_sid + i) % num_memory_;
            if (mn_addr_ptr[sid] >= client_kv_area_limit_) {
                printf("Error addr map %d %d %d\n", block_cnt, sid, st_sid);
                for (int j = 0; j < num_memory_; j ++)
                    printf("server: %lx\n", mn_addr_ptr[j]);
                exit(1);
            }
            if (mn_addr_ptr[sid] & 0xFF != 0) {
                printf("Error addr map addr\n");
                exit(1);
            }
            addr_list[i] = mn_addr_ptr[sid];
            mn_addr_ptr[sid] += allocate_size;
        }
        if (st_sid == my_sid_) {
            allocable_blocks_.push(addr_list[0]);
        }
        block_cnt ++;
    }
}

uint64_t ServerFRMM::mm_alloc() {
    if (allocable_blocks_.size() == 0) {
        return 0;
    }
    uint64_t ret_addr = allocable_blocks_.front();
    allocable_blocks_.pop();
    printf("leave %d blocks~\n", allocable_blocks_.size());
    allocated_blocks_[ret_addr] = true;
    return ret_addr;
}

int ServerFRMM::mm_free(uint64_t st_addr) {
    if (allocated_blocks_[st_addr] != true)
        return -1;
    allocated_blocks_[st_addr] = false;
    allocable_blocks_.push(st_addr);
    return 0;
}

uint64_t ServerFRMM::mm_alloc_small_subtable() { 
    for (size_t i = 0; i < small_subtable_alloc_map_.size(); i ++) {
        if (small_subtable_alloc_map_[i] == 0) {
            small_subtable_alloc_map_[i] = 1;
            return small_subtable_addr + i * roundup_256(SUBTABLE_LEN);
        }
    }
    return 0;
}

uint64_t ServerFRMM::mm_alloc_big_subtable() {
    for (size_t i = 0; i < big_subtable_alloc_map_.size(); i ++) {
        if (big_subtable_alloc_map_[i] == 0) {
            big_subtable_alloc_map_[i] = 1;
            return big_subtable_addr + i * roundup_256(BIG_SUBTABLE_LEN);
        }
    }
    return 0; 
}

uint32_t ServerFRMM::get_rkey() {
    return this->mr_->rkey;
}

int ServerFRMM::get_client_gc_info(uint32_t client_id, __OUT struct MrInfo * mr_info) {
    uint64_t single_gc_len = 1024 * 1024;
    uint64_t client_gc_off = client_id * single_gc_len;
    if (client_gc_off + single_gc_len >= this->client_gc_area_len_) {
        return -1;
    }
    mr_info->addr = this->client_gc_area_off_ + client_gc_off + this->base_addr_;
    mr_info->rkey = this->mr_->rkey;
    return 0;
}

int ServerFRMM::get_mr_info(__OUT struct MrInfo * mr_info) {
    mr_info->addr = this->base_addr_;
    mr_info->rkey = this->mr_->rkey;
    return 0;
}

int ServerFRMM::init_root(void * root_addr) {
    RaceHashRoot * root = (RaceHashRoot *)root_addr;
    root->global_depth = RACE_HASH_GLOBAL_DEPTH;
    root->init_local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
    root->max_global_depth = RACE_HASH_MAX_GLOBAL_DEPTH;
    root->prefix_num = 1 << RACE_HASH_MAX_GLOBAL_DEPTH;
    root->subtable_res_num = root->prefix_num;
    root->subtable_init_num = RACE_HASH_INIT_SUBTABLE_NUM;
    root->subtable_hash_range = RACE_HASH_ADDRESSABLE_BUCKET_NUM;
    root->subtable_bucket_num = RACE_HASH_SUBTABLE_BUCKET_NUM;
    root->seed = rand();
    root->root_offset = client_hash_area_off_;
    root->subtable_offset = root->root_offset + roundup_256(ROOT_RES_LEN) * 3;
    root->kv_offset = client_kv_area_off_;
    root->kv_len = client_kv_area_len_;
    root->lock = 0;
    return 0;
}

int ServerFRMM::init_subtable() {
    small_subtable_addr = get_subtable_addr();
    max_small_subtable = MAX_SMALL_SUBTABLE;
    max_big_subtable = MAX_BIG_SUBTABLE;
    big_subtable_addr = small_subtable_addr + max_small_subtable * roundup_256(SUBTABLE_LEN);
    cout << "max avaliable small subtables:" << max_small_subtable << endl;
    cout << "max avaliable big subtables:" << max_big_subtable << endl;
    
    uint64_t total_subtable_size = max_small_subtable * roundup_256(SUBTABLE_LEN) + 
                                    max_big_subtable * roundup_256(BIG_SUBTABLE_LEN);
    uint64_t end_addr = big_subtable_addr + max_big_subtable * roundup_256(BIG_SUBTABLE_LEN);
    
    printf("Subtable memory layout:\n");
    printf("  small_subtable_addr: 0x%lx\n", small_subtable_addr);
    printf("  big_subtable_addr: 0x%lx\n", big_subtable_addr);
    printf("  end_addr: 0x%lx\n", end_addr);
    printf("  base_addr_: 0x%lx\n", base_addr_);
    printf("  base_len_: 0x%lx\n", base_len_);
    printf("  base_addr_ + base_len_: 0x%lx\n", base_addr_ + base_len_);
    
    if (end_addr > base_addr_ + base_len_) {
        printf("ERROR: Subtable addresses exceed allocated memory!\n");
        printf("  Required end: 0x%lx\n", end_addr);
        printf("  Available end: 0x%lx\n", base_addr_ + base_len_);
        exit(1);
    }
    
    small_subtable_alloc_map_.resize(max_small_subtable);
    for (int i = 0; i < max_small_subtable; i ++) {
        uint64_t cur_subtable_addr = (uint64_t)small_subtable_addr + i * roundup_256(SUBTABLE_LEN);
        small_subtable_alloc_map_[i] = 0;
        // Zero initialize the entire subtable in chunks to avoid AddressSanitizer issues
        uint64_t chunk_size = 4096; // 4KB chunks
        for (uint64_t offset = 0; offset < roundup_256(SUBTABLE_LEN); offset += chunk_size) {
            uint64_t to_clear = (offset + chunk_size <= roundup_256(SUBTABLE_LEN)) ? chunk_size : (roundup_256(SUBTABLE_LEN) - offset);
            memset((void *)(cur_subtable_addr + offset), 0, to_clear);
        }
    }
    big_subtable_alloc_map_.resize(max_big_subtable);
    for (int i = 0; i < max_big_subtable; i ++) {
        uint64_t cur_subtable_addr = (uint64_t)big_subtable_addr + i * roundup_256(BIG_SUBTABLE_LEN);
        big_subtable_alloc_map_[i] = 0;
        // Zero initialize the entire subtable in chunks to avoid AddressSanitizer issues
        uint64_t chunk_size = 4096; // 4KB chunks
        for (uint64_t offset = 0; offset < roundup_256(BIG_SUBTABLE_LEN); offset += chunk_size) {
            uint64_t to_clear = (offset + chunk_size <= roundup_256(BIG_SUBTABLE_LEN)) ? chunk_size : (roundup_256(BIG_SUBTABLE_LEN) - offset);
            memset((void *)(cur_subtable_addr + offset), 0, to_clear);
        }
    }
    return 0;
}

int ServerFRMM::init_hashtable() {
    printf("init_hashtable() starting\n");
    printf("  client_hash_area_off_: 0x%lx\n", client_hash_area_off_);
    printf("  base_addr_: 0x%lx\n", base_addr_);
    
    kv_ec_meta_root_addr = get_kv_ec_meta_addr();
    printf("  kv_ec_meta_root_addr: 0x%lx\n", kv_ec_meta_root_addr);
    
    object_index_addr = get_obj_addr();
    printf("  object_index_addr: 0x%lx\n", object_index_addr);
    
    stripe_index_addr = get_str_addr();
    printf("  stripe_index_addr: 0x%lx\n", stripe_index_addr);
    
    chunk_index_addr = get_chu_addr();
    printf("  chunk_index_addr: 0x%lx\n", chunk_index_addr);
    
    printf("  roundup_256(ROOT_KV_META_LEN) = 0x%lx\n", roundup_256(ROOT_KV_META_LEN));
    printf("  roundup_256(MAX_OBJ_LEN) = 0x%lx\n", roundup_256(MAX_OBJ_LEN));
    printf("  roundup_256(MAX_STR_LEN) = 0x%lx\n", roundup_256(MAX_STR_LEN));
    printf("  roundup_256(MAX_CHU_LEN) = 0x%lx\n", roundup_256(MAX_CHU_LEN));
    
    init_subtable();
    return 0;
}

uint64_t ServerFRMM::get_kv_ec_meta_addr(){
    return client_hash_area_off_ + base_addr_;
}

uint64_t ServerFRMM::get_obj_addr(){
    return client_hash_area_off_ + base_addr_ + roundup_256(ROOT_KV_META_LEN);
}

uint64_t ServerFRMM::get_str_addr(){
    return client_hash_area_off_ + base_addr_ + roundup_256(ROOT_KV_META_LEN) + roundup_256(MAX_OBJ_LEN);
}

uint64_t ServerFRMM::get_chu_addr(){
    return client_hash_area_off_ + base_addr_ + roundup_256(ROOT_KV_META_LEN) + roundup_256(MAX_OBJ_LEN) +
        roundup_256(MAX_STR_LEN);
}

uint64_t ServerFRMM::get_subtable_addr(){
    return client_hash_area_off_ + base_addr_ + roundup_256(ROOT_KV_META_LEN) + roundup_256(MAX_OBJ_LEN) +
        roundup_256(MAX_STR_LEN) + roundup_256(MAX_CHU_LEN);
}