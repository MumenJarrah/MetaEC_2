#ifndef CIRCULAR_QUEUE_H
#define CIRCULAE_QUEUE_H

#include <iostream>
#include <vector>

template <typename T>
class CircularQueue {
private:
    std::vector<T> queue;
    size_t capacity;
    size_t frontIndex;
    size_t rearIndex;
    size_t itemCount;

public:
    CircularQueue(size_t capacity) : capacity(capacity), frontIndex(0), rearIndex(0), itemCount(0) {
        queue.resize(capacity);
    }

    void enqueue(const T& item) {
        if (itemCount == capacity) {
            std::cerr << "Queue is full. Cannot enqueue." << std::endl;
            return;
        }

        queue[rearIndex] = item;
        rearIndex = (rearIndex + 1) % capacity;
        itemCount++;
    }

    T dequeue() {
        if (itemCount == 0) {
            std::cerr << "Queue is empty. Cannot dequeue." << std::endl;
            exit(EXIT_FAILURE);
        }

        T item = queue[frontIndex];
        frontIndex = (frontIndex + 1) % capacity;
        itemCount--;
        return item;
    }

    void freequeue() {
        rearIndex = (rearIndex + 1) % capacity;
        itemCount++;
    }

    T dequeue_free() {
        if (itemCount == 0) {
            std::cerr << "Queue is empty. Cannot dequeue." << std::endl;
            exit(EXIT_FAILURE);
        }

        T item = queue[frontIndex];
        frontIndex = (frontIndex + 1) % capacity;
        rearIndex = (rearIndex + 1) % capacity;
        return item;
    }

    void clear() {
        frontIndex = rearIndex;
        itemCount = 0;
    }

    size_t size() const {
        return itemCount;
    }

    bool is_empty() const {
        return itemCount == 0;
    }

    bool is_full() const {
        return itemCount == capacity;
    }

    std::vector<T> pick_data(size_t startIndex, size_t endIndex) const {

        std::vector<T> pickedData;
        size_t index = startIndex;
        while (index != (endIndex + 1) % capacity) {
            pickedData.push_back(queue[(index) % capacity]);
            index = (index + 1) % capacity;
        }
        return pickedData;
    }

    size_t get_front_index() {
        return frontIndex;
    }

    size_t get_rear_index() {
        return rearIndex;
    }

};


#endif