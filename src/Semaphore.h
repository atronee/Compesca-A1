#include <mutex>
#include <condition_variable>

class Semaphore {
private:
    std::mutex mtx;
    std::condition_variable cv;
    int count;
    int max_count;

public:
    // Initializes the semaphore with a count and a maximum count.
    // If no arguments are provided, the count is set to 0 and the maximum count is set to 1, making it a binary semaphore.
    explicit Semaphore(int count = 0, int max_count = 1) : count(count), max_count(max_count) {}

    void notify();

    void wait();
};

