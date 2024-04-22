
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <mutex>
#include "DataFrameVersionManager.h"

using std::vector;
using std::string;

class Handler {
protected:
    ConsumerProducerQueue<DataFrame*> *queue_in;
    ConsumerProducerQueue<DataFrame*> *queue_out;
public:
    Handler(ConsumerProducerQueue<DataFrame*> *queue_in, ConsumerProducerQueue<DataFrame*> *queue_out):
    queue_in(queue_in), queue_out(queue_out){};
};

class SelectHandler : public Handler {
public:
    SelectHandler(ConsumerProducerQueue<DataFrame*> *queue_in, ConsumerProducerQueue<DataFrame*> *queue_out)
            : Handler(queue_in, queue_out) {};
    void select(vector<string> columns);
};

class FilterHandler : public Handler {
public:
    FilterHandler(ConsumerProducerQueue<DataFrame*> *queue_in, ConsumerProducerQueue<DataFrame*> *queue_out)
    : Handler(queue_in, queue_out) {};

    void filter(string column, string operation, string value);
};

class GroupByHandler : public Handler {
public:
    GroupByHandler(ConsumerProducerQueue<DataFrame*> *queue_in, ConsumerProducerQueue<DataFrame*> *queue_out)
    : Handler(queue_in, queue_out) {};

    void group_by(string column, string operation);
};

class SortHandler : public Handler {
public:
    SortHandler(ConsumerProducerQueue<DataFrame*> *queue_in, ConsumerProducerQueue<DataFrame*> *queue_out)
    : Handler(queue_in, queue_out) {};

    void sort(string column, string order);
};

class printHandler : public Handler {
private:
    std::mutex print_mtx;
public:
    printHandler(ConsumerProducerQueue<DataFrame*> *queue_in, ConsumerProducerQueue<DataFrame*> *queue_out= nullptr)
    : Handler(queue_in, queue_out) {};

    void print();
};

class JoinHandler : public Handler {

private:
    void join_int(DataFrame* df1, string main_column, string join_column);
public:
    JoinHandler(ConsumerProducerQueue<DataFrame*> *queue_in, ConsumerProducerQueue<DataFrame*> *queue_out)
    : Handler(queue_in, queue_out) {};


    void join(DataFrame* df1, string main_column, string join_column);

    void join(DataFrameVersionManager* dfvm, string main_column, string join_column);

};

class FinalHandler : public Handler {
public:
    FinalHandler(ConsumerProducerQueue<DataFrame*> *queue_in, ConsumerProducerQueue<DataFrame*> *queue_out)
    : Handler(queue_in, queue_out) {};

    void aggregate();
};