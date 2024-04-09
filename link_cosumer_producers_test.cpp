#include <mutex>
#include <semaphore>
#include <queue>
#include <thread>
#include <iostream>
using namespace std;

mutex m;
queue<int> q;
queue<int> q2;
mutex m2;
mutex m0;
std::counting_semaphore<10> emptySemaphore(10);
std::counting_semaphore<10> fullSemaphore(0);
std::counting_semaphore<10> emptySemaphore2(10);
std::counting_semaphore<10> fullSemaphore2(0);

void producer() {
    for(int i=0; i<1000; i++) {
        emptySemaphore.acquire();
        m.lock();
        q.push(i);
        m.unlock();
        fullSemaphore.release();
    }
}

void consumer() {
    while(true) {
        fullSemaphore.acquire();
        m.lock();
        int i = q.front();

        q.pop();
        m.unlock();
        emptySemaphore.release();
        m0.lock();
        cout << "Consumed: " << i << endl;
        m0.unlock();
        emptySemaphore2.acquire();
        m2.lock();
        q2.push((i+1)*1000);
        m2.unlock();
        fullSemaphore2.release();
    }
}

void consumer2() {
    while(true) {
        fullSemaphore2.acquire();
        m2.lock();
        int i = q2.front();
        q2.pop();
        m2.unlock();
        emptySemaphore2.release();
        m0.lock();
        cout << "Consumed2: " << i << endl;
        m0.unlock();
    }
}
int main() {
    vector <thread> threads;
    vector <thread> threads2;
    thread t1(producer);
    for (int i=0; i<10; i++) {
        threads.push_back(thread(consumer));
        threads2.push_back(thread(consumer2));
    }

    t1.join();
    for(auto &t: threads) {
        t.join();
    }
    for(auto &t: threads2) {
        t.join();
    }
    return 0;
}