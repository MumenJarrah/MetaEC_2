#include "super_queue.h"

SuperQueue::SuperQueue(){
    max_length = MAX_QUEUE_LENGTH;
    huge_queue = (type_ *) malloc (sizeof(type_) * max_length);
    front = 0;
    end = 0;
}

SuperQueue::~SuperQueue(){
    free(huge_queue);
}

void SuperQueue::push_value(type_ data){
    if(end + 1 < max_length){
        huge_queue[end] = data;
        end ++;
    }
    else {
        cout<<"no space!\n";
    }
}

bool SuperQueue::is_empty(){
    return (front == end) ? true : false;
}

bool SuperQueue::pop_value(){
    if(!is_empty()){
        huge_queue[front] = -1;
        front ++;
        return true;
    }
    else {
        cout<<"super queue is empty!\n";
        return false;
    }
}

void SuperQueue::print_value(){
    cout<<"print the queue value......\n";
    for(int i = front;i < end;i ++){
        cout<<huge_queue[i];
        if(i < end - 1){
            cout<<" ";
        }
    }
    cout<<endl<<"print end......\n";
}

type_ SuperQueue::get_front(){
    if(!is_empty()){
        return huge_queue[front];
    }
    else {
        cout<<"queue is empty!\n";
        return -1;
    }
}

type_ SuperQueue::get_end(){
    if(!is_empty()){
        return huge_queue[end - 1];
    }
    else {
        cout<<"queue is empty!\n";
        return -1;
    }
}

void SuperQueue::clear_queue(){
    front = end;
}

int SuperQueue::get_size(){
    return end - front;
}

type_ SuperQueue::get_value_byid(int index){
    if(index < get_size()){
        return huge_queue[front + index];
    }
    else {
        cout << "illegal index!\n";
        return -1;
    }
}