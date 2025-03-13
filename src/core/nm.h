#ifndef DDCKV_NM_H
#define DDCKV_NM_H

#include "ib.h"

#include <infiniband/verbs.h>
#include <stdint.h>
#include <netdb.h>
#include <vector>
#include <map>
#include <mutex>
#include <thread>
#include <boost/fiber/all.hpp>
#include <tbb/concurrent_hash_map.h>
#include <unordered_set>

#define MAX_WC_LIST 100
#define TIME_RANGE 3
using namespace std;
#include <boost/coroutine/all.hpp>

class UDPNetworkManager {
private:
    uint32_t udp_sock_;
    uint16_t udp_port_;
    uint8_t  role_;
    uint8_t  conn_type_;
    struct sockaddr_in * server_addr_list_;
    uint32_t num_server_;
    uint32_t server_id_;
    struct ibv_context   * ib_ctx_;
    struct ibv_pd        * ib_pd_;
    struct ibv_cq        * ib_cq_;
    uint8_t                ib_port_num_;
    struct ibv_port_attr   ib_port_attr_;
    struct ibv_device_attr ib_device_attr_;
    union  ibv_gid       * ib_gid_;
    std::vector<struct MrInfo *> mr_info_list_;
    int udp_init_client(const struct GlobalConfig * conf);
    int udp_init_server(const struct GlobalConfig * conf);

// private methods
private:
    struct ibv_qp * server_create_rc_qp();
    struct ibv_qp * client_create_rc_qp();
    int  get_qp_info(struct ibv_qp * qp, __OUT struct QpInfo * qp_info);

// inline public functions
public:

    volatile bool stop_polling_;
    tbb::concurrent_hash_map<uint64_t, struct ibv_wc *> wrid_wc_map_;
    tbb::concurrent_hash_map<uint64_t, double> wrid_wc_map_timer_;
    unordered_set<uint64_t> wrid_wc_set_;
    std::vector<struct ibv_qp *> rc_qp_list_;
    inline uint32_t get_one_server_id(uint32_t hint) {
        return hint % num_server_;
    }

    inline uint32_t get_server_rkey(uint8_t server_id) {
        return mr_info_list_[server_id]->rkey;
    }

    inline uint32_t get_server_id()  {
        return server_id_;
    }

    inline uint16_t get_server_udp() {
        return udp_port_;
    }

    inline uint32_t get_num_servers() {
        return num_server_;
    }

public:
    UDPNetworkManager(const struct GlobalConfig * conf);
    ~UDPNetworkManager();

    bool is_all_complete(const std::map<uint64_t, bool> & wr_id_comp_map);
    // common udp functions
    int nm_recv_udp_msg(__OUT struct KVMsg * kvmsg, 
        __OUT struct sockaddr_in * src_addr, __OUT socklen_t * src_addr_len);
    int nm_send_udp_msg(struct KVMsg * kvmsg, struct sockaddr_in * dest_addr,
        socklen_t dest_addr_len);
    int nm_send_udp_msg_to_server(struct KVMsg * kvmsg, uint32_t server_id);
    void close_udp_sock();
    // common ib functions
    void get_ib_info(__OUT struct IbInfo * ib_info);
    int  rdma_post_send_batch_async(uint32_t server_id, struct ibv_send_wr * wr_list);
    int  rdma_post_sr_list_batch_sync(std::vector<IbvSrList *> & sr_list_batch,
        std::vector<uint32_t> & sr_list_num_batch, __OUT struct ibv_wc * wc);
    int  rdma_poll_one_completion(struct ibv_wc * wc);
    int  nm_check_completion(std::map<uint64_t, bool> & wrid_wc_map);
    int  nm_rdma_write_inl_to_sid(void * data, uint32_t size, uint64_t remote_addr, 
            uint32_t remote_rkey, uint32_t server_id);
    int  nm_rdma_read_from_sid(void * local_addr, uint32_t local_lkey, 
            uint32_t size, uint64_t remote_addr, uint32_t remote_rkey, uint32_t server_id);
    int  nm_rdma_write_to_sid(void * local_addr, uint32_t local_lkey, 
            uint32_t size, uint64_t remote_addr, uint32_t remote_rkey, uint32_t server_id);
    int nm_rdma_cas(void * local_addr, uint32_t local_lkey, 
        uint32_t size, uint64_t remote_addr, uint32_t remote_rkey, uint32_t server_id,
        uint64_t swap_value, uint64_t cmp_value);
    int nm_rdma_faa(void * local_addr, uint32_t local_lkey, 
        uint32_t size, uint64_t remote_addr, uint32_t remote_rkey, uint32_t server_id,
        uint64_t add_value);
    // for server
    int nm_on_connect_new_qp(const struct KVMsg * request, __OUT struct QpInfo * qp_info);
    int nm_on_connect_connect_qp(uint32_t client_id, 
        const struct QpInfo * local_qp_info, 
        const struct QpInfo * remote_qp_info);
    // for client
    int client_connect_one_rc_qp(uint32_t server_id, __OUT struct MrInfo * mr_info);
    // for polling thread
    void nm_thread_polling();
    void stop_polling();
    int rdma_post_check_sr_list(map<uint64_t, bool>  *comp_wrid_map);
    int rdma_post_sr_list_batch_sync_send(vector<IbvSrList *> & sr_list_batch,
        vector<uint32_t> & sr_list_num_batch, map<uint64_t, bool> & comp_wrid_map);
};

typedef struct TagNMPollingThreadArgs {
    UDPNetworkManager * nm;
    int core_id;
} NMPollingThreadArgs;
void * nm_polling_thread(void * args);

#endif