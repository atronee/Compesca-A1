#include <mutex>
#include <condition_variable>
#include "Semaphore.h"

void Semaphore::notify() {
    std::unique_lock<std::mutex> lock(mtx);
    while (count == max_count)
        cv.wait(lock);
    count++;
    cv.notify_one();
}

void Semaphore::wait() {
    std::unique_lock<std::mutex> lock(mtx);
    while (count == 0)
        cv.wait(lock);
    count--;
}