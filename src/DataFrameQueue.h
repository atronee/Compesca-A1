#include <queue>
#include <mutex>
#include "DataFrame.h"
#include "Semaphore.h"

class DataFrameQueue {
    // A queue of DataFrame objects. The queue has a maximum size of 100.
    // The queue is thread-safe.
private:
    std::queue<DataFrame> dataQueue;
    Semaphore emptySemaphore;
    Semaphore fullSemaphore;
public:
    DataFrameQueue(int max_count = 100) : emptySemaphore(max_count, max_count), fullSemaphore(0, max_count) {};
    std::mutex mtx;
    void push(DataFrame df);
    DataFrame pop();
    bool is_empty();
    int size();

};

