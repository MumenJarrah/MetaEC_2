#include "fragec/serverfr.h"

int main(int argc, char ** argv) {
    if (argc != 3) {
        printf("Usage: %s path-to-config-file [server id]\n", argv[0]);
        return 1;
    }
    printf("===== Starting Config =====\n");
    int32_t server_id = atoi(argv[2]);
    int ret = 0;
    GlobalConfig server_conf;
    ret = load_config(argv[1], &server_conf);
    server_conf.server_id = server_id;
    ServerFR * server = new ServerFR(&server_conf);
    ServerFRMainArgs server_main_args;
    server_main_args.server = server;
    server_main_args.core_id = server_conf.main_core_id;
    pthread_t server_tid;
    pthread_create(&server_tid, NULL, serverfr_main, (void *)&server_main_args);
    printf("===== Starting Server %d =====\n", server_conf.server_id);
    sleep(100000000ll);
    server->stop();
    return 0;
}