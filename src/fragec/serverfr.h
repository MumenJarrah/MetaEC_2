#ifndef DDCKV_SERVERFR_H_
#define DDCKV_SERVERFR_H_

#include "core/nm.h"
#include "fragec/serverfr_mm.h"
#include "ec_log/ec_log.h"

#include <assert.h>
#include <arpa/inet.h>

class ServerFR {
    uint32_t server_id_;
    volatile uint8_t need_stop_;
    UDPNetworkManager * nm_;
    ServerFRMM * mm_;
    bool if_print_log;
    
public:
    ServerFR(const struct GlobalConfig * conf);
    ~ServerFR();
    int server_on_connect(const struct KVMsg * request, struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int server_on_alloc(const struct KVMsg * request, struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int server_on_alloc_small_subtable(const struct KVMsg * request, struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int server_on_alloc_big_subtable(const struct KVMsg * request, struct sockaddr_in * src_addr, socklen_t src_addr_len);
    void * thread_main();
    void stop();
};

typedef struct TagServerFRMainArgs {
    ServerFR * server;
    int      core_id;
} ServerFRMainArgs;

void * serverfr_main(void * server_main_args);

#endif