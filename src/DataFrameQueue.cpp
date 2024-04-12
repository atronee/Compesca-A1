
#include <queue>
#include <mutex>
#include "DataFrameQueue.h"



void DataFrameQueue::push(DataFrame df){
    emptySemaphore.wait();
    mtx.lock();
    dataQueue.push(df);
    mtx.unlock();
    fullSemaphore.notify();
}

DataFrame DataFrameQueue::pop(){
    fullSemaphore.wait();
    mtx.lock();
    DataFrame df = dataQueue.front();
    dataQueue.pop();
    mtx.unlock();
    emptySemaphore.notify();
    return df;
}

bool DataFrameQueue::is_empty(){
    return dataQueue.empty();
}

int DataFrameQueue::size(){
    return dataQueue.size();
}


