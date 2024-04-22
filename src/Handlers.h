
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <mutex>

using std::vector;
using std::string;

class Handler {
protected:
    ConsumerProducerQueue<DataFrame*> *queue_in;
    vector<ConsumerProducerQueue<DataFrame*>> *queues_out;
public:
    Handler(ConsumerProducerQueue<DataFrame*> *queue_in, vector<ConsumerProducerQueue<DataFrame*>> *queues_out):
    queue_in(queue_in), queues_out(queues_out){};
};

class SelectHandler : public Handler {
public:
    SelectHandler(ConsumerProducerQueue<DataFrame*> *queue_in, vector<ConsumerProducerQueue<DataFrame*>> *queues_out)
            : Handler(queue_in, queues_out) {};
    void select(vector<string> columns);
};

class FilterHandler : public Handler {
public:
    FilterHandler(ConsumerProducerQueue<DataFrame*> *queue_in, vector<ConsumerProducerQueue<DataFrame*>> *queues_out)
    : Handler(queue_in, queues_out) {};

    void filter(string column, string operation, string value);
};

class GroupByHandler : public Handler {
public:
    GroupByHandler(ConsumerProducerQueue<DataFrame*> *queue_in, vector<ConsumerProducerQueue<DataFrame*>> *queues_out)
    : Handler(queue_in, queues_out) {};

    void group_by(string column, string operation);
};

class SortHandler : public Handler {
public:
    SortHandler(ConsumerProducerQueue<DataFrame*> *queue_in, vector<ConsumerProducerQueue<DataFrame*>> *queues_out)
    : Handler(queue_in, queues_out) {};

    void sort(string column, string order);
};

class printHandler : public Handler {
private:
    std::mutex print_mtx;
public:
    printHandler(ConsumerProducerQueue<DataFrame*> *queue_in, vector<ConsumerProducerQueue<DataFrame*>> *queues_out= nullptr)
    : Handler(queue_in, queues_out) {};

    void print();
};

class JoinHandler : public Handler {

private:
    void join_int(DataFrame* df1, string main_column, string join_column);
public:
    JoinHandler(ConsumerProducerQueue<DataFrame*> *queue_in, vector<ConsumerProducerQueue<DataFrame*>> *queues_out)
    : Handler(queue_in, queues_out) {};


    void join(DataFrame* df1, string main_column, string join_column);

};