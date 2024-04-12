#ifndef CONSUMERPRODUCERQUEUE_H
#define CONSUMERPRODUCERQUEUE_H
#include <queue>
#include <mutex>
#include "DataFrame.h"
#include "Semaphore.h"

template <typename T>
class ConsumerProducerQueue {
    // A Thread-safe Queue.
    // The queue uses two semaphores to control access to the queue, implementing the producer-consumer pattern.
private:
    std::queue<T> dataQueue;
    Semaphore emptySemaphore;
    Semaphore fullSemaphore;
public:
    explicit ConsumerProducerQueue(int max_count = 100) : emptySemaphore(max_count, max_count), fullSemaphore(0, max_count) {};
    std::mutex mtx;


    void push(T data){
        emptySemaphore.wait();
        mtx.lock();
        dataQueue.push(data);
        mtx.unlock();
        fullSemaphore.notify();
    }

    T pop(){
        fullSemaphore.wait();
        mtx.lock();
        T data = dataQueue.front();
        dataQueue.pop();
        mtx.unlock();
        emptySemaphore.notify();
        return data;
    }

    bool is_empty(){
        return dataQueue.empty();
    }

    int size(){
        return dataQueue.size();
    }

};

#endif // CONSUMERPRODUCERQUEUE_H