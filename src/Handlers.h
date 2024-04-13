
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>

using std::vector;
using std::string;

class Handler {
public:
    Handler() {}
    ~Handler() {}
    ConsumerProducerQueue<DataFrame*> *queue_in;
    ConsumerProducerQueue<DataFrame*> *queue_out;
    DataFrame* df = queue_in->pop();
};

class SelectHandler : public Handler {
public:
    SelectHandler() {}
    ~SelectHandler() {}
    DataFrame* select(vector<string> columns);
};

class FilterHandler : public Handler {
public:
    FilterHandler() {}
    ~FilterHandler() {}
    DataFrame* filter(string column, string operation, string value);
};