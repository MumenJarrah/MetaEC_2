#ifndef DDCKV_SERVER_H_
#define DDCKV_SERVER_H_

#include "core/nm.h"
#include "memory_pool/server_mm.h"
#include "ec_log/ec_log.h"

#include <assert.h>
#include <arpa/inet.h>

class Server {
    uint32_t server_id_;
    volatile uint8_t need_stop_;
    UDPNetworkManager * nm_;
    ServerMM * mm_;
    bool if_print_log;
    
public:
    Server(const struct GlobalConfig * conf);
    ~Server();
    int server_on_connect(const struct KVMsg * request, struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int server_on_alloc(const struct KVMsg * request, struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int server_on_alloc_small_subtable(const struct KVMsg * request, struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int server_on_alloc_big_subtable(const struct KVMsg * request, struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int server_on_alloc_log(const struct KVMsg * request, struct sockaddr_in * src_addr, socklen_t src_addr_len);
    void * thread_main();
    void stop();
};

typedef struct TagServerMainArgs {
    Server * server;
    int      core_id;
} ServerMainArgs;

void * server_main(void * server_main_args);

#endif