//
// Created by vini on 22/04/24.
//

#include "DataFrameVersionManager.h"
#include <memory>
#include <map>
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <mutex>
#include <condition_variable>

void DataFrameVersionManager::storeVersion(std::unique_ptr<DataFrame> df) {
    std::chrono::time_point<std::chrono::high_resolution_clock> time = df->get_creation_time();
    std::lock_guard<std::mutex> lock(mtx);
    versions[time] = std::move(df);
    cv.notify_all();  // Notify all waiting threads that a dataframe has been added
}

DataFrame* DataFrameVersionManager::getVersionBefore(std::chrono::time_point<std::chrono::high_resolution_clock> time){
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this]{ return !versions.empty(); });  // Wait until versions is not empty
    auto it = versions.upper_bound(time);
    if (it == versions.begin()) {
        // if no version was created before the specified time, return the first version
        return versions.begin()->second.get();
    }

    // Decrement the iterator to get the latest version created before the specified time
    --it;
    return it->second.get();
}

void DataFrameVersionManager::storeFromQueue() {
    DataFrame* df;
    while ((df = queue_in->pop()) != nullptr) {
        storeVersion(std::unique_ptr<DataFrame>(df));
    }
}