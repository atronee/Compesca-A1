//
// Created by vini on 11/04/24.
//
#include <queue>
#include <mutex>
#include <semaphore>
#include "DataFrameQueue.h"

void DataFrameQueue::push(DataFrame df){
    empty.acquire();
    mtx.lock();
    dataQueue.push(df);
    mtx.unlock();
    full.release();
}

DataFrame DataFrameQueue::pop(){
    full.acquire();
    mtx.lock();
    DataFrame df = dataQueue.front();
    dataQueue.pop();
    mtx.unlock();
    empty.release();
    return df;
}

bool DataFrameQueue::is_empty(){
    return dataQueue.empty();
}

int DataFrameQueue::size(){
    return dataQueue.size();
}

void DataFrameQueue::clear(){
    while(!dataQueue.empty()){
        dataQueue.pop();
    }
}

DataFrameQueue::~DataFrameQueue(){
    clear();
}

