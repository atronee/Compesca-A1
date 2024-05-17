//
// Created by vini on 22/04/24.
//

#ifndef COMPESCA_A1_DATAFRAMEVERSIONMANAGER_H
#define COMPESCA_A1_DATAFRAMEVERSIONMANAGER_H

#include <memory>
#include <map>
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <mutex>
#include <chrono>
#include <condition_variable>

class DataFrameVersionManager {
private:
    std::map<std::chrono::time_point<std::chrono::high_resolution_clock>, std::unique_ptr<DataFrame>> versions;
    ConsumerProducerQueue<DataFrame*> *queue_in;
    mutable std::mutex mtx;
    std::condition_variable cv;  // Condition variable to wait for a dataframe

public:
    explicit DataFrameVersionManager(ConsumerProducerQueue<DataFrame*> *queue_in): queue_in(queue_in) {};

    // Store a new version of the DataFrame
    void storeVersion(std::unique_ptr<DataFrame> df);

    void storeFromQueue();

    // Get the latest version of the DataFrame that was created before the specified time
    DataFrame* getVersionBefore(std::chrono::time_point<std::chrono::high_resolution_clock> time);

    unsigned int getNumVersions() const {
        return versions.size();
    }


};


#endif //COMPESCA_A1_DATAFRAMEVERSIONMANAGER_H
