#include "ec_encoding/buffer.h"

uint64_t char_to_int(unsigned char *str1, int len){
    uint64_t ret_value = 0;
    for(int i = 0;i < len;i ++){
        ret_value += str1[len - 1 - i] * pow(256, i);
    }
    return ret_value;
}

void int_to_char(uint64_t value, unsigned char *key_len_buf, int len){
    uint64_t pre_value = value;
    for(int i = 0;i < len;i ++){
        key_len_buf[i] = int(pre_value / pow(256, len - 1 - i));
        pre_value = pre_value % int(pow(256, len - 1 - i));
    }
}

Buffer::Buffer(int num_buffer, int block_size, int kv_alloc_size){
    this->num_buffer = num_buffer;
    this->block_size = block_size;
    this->kv_alloc_size = kv_alloc_size;
    buffer_list = new SmallBuffer[num_buffer];
    for(int i = 0;i < num_buffer;i ++){
        buffer_list[i].block_off = 0;
        buffer_list[i].capacity = MAX_QUEUE;
        buffer_list[i].kv_queue = new CircularQueue<KvBuffer>(MAX_QUEUE);
        buffer_list[i].encoding_queue = new queue<EncodingMessage>;
    }
}

Buffer::~Buffer(){
    for(int i = 0;i < num_buffer;i ++){
        delete(buffer_list[i].kv_queue);
        delete(buffer_list[i].encoding_queue);
    }
    delete(buffer_list);
}

void Buffer::insert(int buffer_id, string key, uint64_t ptr, int kv_size, uint64_t hash_key, 
    KvEcMetaMes kv_mes, uint8_t fp){
    SmallBuffer *buffer_ = &buffer_list[buffer_id];
    int add_size = get_aligned_size(kv_size, kv_alloc_size);
    KvBuffer kv_buffer;
    kv_buffer.key      = key;
    kv_buffer.kv_addr  = ptr;
    kv_buffer.kv_len   = kv_size;
    kv_buffer.hash_key = hash_key;
    kv_buffer.kv_mes   = kv_mes;
    kv_buffer.fp       = fp;
    // check if need encoding
    if(buffer_->block_off + add_size > block_size){
        EncodingMessage enc_mes;
        enc_mes.buffer_id   = buffer_id;
        enc_mes.start_index = buffer_->kv_queue->get_front_index();
        enc_mes.end_index   = buffer_->kv_queue->get_rear_index() - 1;
        buffer_->block_off  = 0;
        buffer_->encoding_queue->push(enc_mes);
        buffer_->kv_queue->clear();
    } 
    buffer_->kv_queue->enqueue(kv_buffer);
    buffer_->block_off += add_size;
}

void Buffer::print_encoding_mes(){
    cout << "print encoding mes~" << endl;
    SmallBuffer *sbuffer;
    for(int i = 0;i < num_buffer;i ++){
        sbuffer = &buffer_list[i];
        cout << "buffer:[" << i << "] have " << sbuffer->encoding_queue->size() << " need encoding~" << endl;
    }
    cout << "print encoding mes finished~" << endl;
}

void Buffer::print_buffer_mes(){
    cout << "print buffer mes~" << endl;
    SmallBuffer *sbuffer;
    int leave_kv;
    for(int i = 0;i < num_buffer;i ++){
        sbuffer = &buffer_list[i];
        leave_kv = sbuffer->kv_queue->size();
        cout << "buffer:[" << i << "] have " << leave_kv << " no queue encoding~" << endl;
    }
    cout << "print buffer mes finished~" << endl;
}

void Buffer::print_buffer_used_size(){
    cout << "print buffer used size~" << endl;
    SmallBuffer *sbuffer;
    for(int i = 0;i < num_buffer;i ++){
        sbuffer = &buffer_list[i];
        cout << "buffer:[" << i << "] buffer used size:" << sbuffer->block_off << endl;
    }
    cout << "print buffer used size finished~" << endl;
}

void Buffer::print_mes(){
    cout << "print mes of the buffer----------------------" << endl;
    cout << "buffer block size:" << block_size << endl;
    print_buffer_used_size();
    print_buffer_mes();
    print_encoding_mes();
    cout << "print mes of the buffer finished-------------" << endl;
}

vector<KvBuffer> Buffer::pick_data(int buffer_id, int start_index, int end_index){
    return buffer_list[buffer_id].kv_queue->pick_data(start_index, end_index);
}