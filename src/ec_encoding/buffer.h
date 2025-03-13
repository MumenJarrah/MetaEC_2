#ifndef BUFFER
#define BUFFER

#include "core/circular_queue.h"
#include <string>
#include <queue>
#include "core/kv_utils.h"

#define MAX_QUEUE 100000
#define BIT_MAP_SIZE 32
#define METADATA_LEN_SIZE 2

#define KV_KEY_LEN   sizeof(KvKeyLen)
#define KV_VALUE_LEN sizeof(KvValueLen)   
#define KV_TAIL_LEN  sizeof(KvTail)
#define KV_OTHER_LEN (KV_KEY_LEN + KV_VALUE_LEN + KV_TAIL_LEN)

#define MAX_KEY_SIZE 256
// #define MAX_KEY_SIZE 256 twitter
#define MAX_VALUE_SIZE 4096
#define MAX_OP_SIZE  8

using namespace std;

struct KvEcMetaMes {
    uint8_t server_id[MAX_REP_NUM];
    uint64_t slot_addr[MAX_REP_NUM];

};

struct KvKeyLen{
    unsigned char key_len[2];
};

struct KvValueLen{
    unsigned char value_len[4];
};

struct KvTail{
    unsigned char crc[1];
};

struct EcMeta{
    char key[MAX_KEY_SIZE];
    uint16_t key_len;
};

struct KvBuffer{
    uint64_t kv_addr;
    string   key;
    int      kv_len;
    uint64_t hash_key;
    uint8_t  fp;
    KvEcMetaMes kv_mes;
};

struct EncodingMessage{
    int buffer_id;
    int start_index;
    int end_index;
};

struct SmallBuffer {
    int capacity;
    CircularQueue<KvBuffer> *kv_queue;
    int block_off;
    queue <EncodingMessage> *encoding_queue;
};

class Buffer {

    public:
        Buffer(int num_buffer, int block_size, int kv_alloc_size);
        ~Buffer();
        void insert(int buffer_id, string key, uint64_t ptr, int kv_size, uint64_t hash_key, KvEcMetaMes kv_mes, uint8_t fp);
        vector<KvBuffer> pick_data(int buffer_id, int start_index, int end_index);
        void print_encoding_mes();
        void print_buffer_mes();
        void print_buffer_used_size();
        void print_mes();

    public:
        int num_buffer;
        int block_size;
        int kv_alloc_size;
        SmallBuffer *buffer_list;
};

uint64_t char_to_int(unsigned char *str1, int len); // turn the str to int, length is len
void int_to_char(uint64_t value, unsigned char *key_len_buf, int len); // turn the int to char, length is len

#endif