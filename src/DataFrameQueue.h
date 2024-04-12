#include <queue>
#include <mutex>
#include <semaphore>
#include "DataFrame.h"

class DataFrameQueue {
    // A queue of DataFrame objects. The queue has a maximum size of 100.
    // The queue is thread-safe.
private:
    std::queue<DataFrame> dataQueue;
    std::counting_semaphore<100> empty{100};
    std::counting_semaphore<100> full{0};
public:
    std::mutex mtx;
    void push(DataFrame df);
    DataFrame pop();
    bool is_empty();
    int size();
    void clear();
    ~DataFrameQueue();
};

