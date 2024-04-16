#ifndef READER_H                   
#define READER_H

#include <vector>                 
#include <string>
#include <map>
#include <unordered_map>
#include <any>
#include <typeinfo> 
#include "ConsumerProducerQueue.h"

class Reader {                    // Declaration of the base class Reader
public:
    // read function to read data from a file
virtual void read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter, ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>>& queue, bool read_in_blocks) = 0;};

class FileReader : public Reader {  // Declaration of the derived class CSVReader, inheriting from Reader
public:
    void read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter, ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>>& queue, bool read_in_blocks) override;
    // Implementation of the read function for reading CSV files
    // Parameters: filename - name of the CSV file to read from
    //             types - vector of type_info pointers specifying the types of data to read
    //             delimiter - character used as a delimiter in the CSV file (default: ',')
    //             queue - reference to the queue to store the data
    //             read_in_blocks - flag to indicate whether to read data in blocks or not (default: true)
};

#endif // READER_H               // End of header guard