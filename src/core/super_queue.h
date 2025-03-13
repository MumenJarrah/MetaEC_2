#ifndef DDCKV_SUPER_QUEUE
#define DDCKV_SUPER_QUEUE

#include<stdint.h>
#include<malloc.h>
#include<iostream>

#define MAX_QUEUE_LENGTH 1000000
typedef uint64_t type_;

using namespace std;

class SuperQueue{
    public:
        int front;
        int end;
        int max_length;
        uint64_t *huge_queue;
    
    public:
        SuperQueue();
        ~SuperQueue();
        void push_value(type_ data);
        bool is_empty();
        bool pop_value();
        void print_value();
        type_ get_front();
        type_ get_end();
        void clear_queue();
        int get_size();
        type_ get_value_byid(int index);
};

#endif