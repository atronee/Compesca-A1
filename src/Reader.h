#ifndef READER_H                   
#define READER_H

#include "DataFrame.h"
#include <vector>                 
#include <string>
#include <map>
#include <unordered_map>
#include <any>
#include <typeinfo> 
#include "ConsumerProducerQueue.h"

class Reader { // Declaration of the base class Reader
public:
    // read function to read data from a file
virtual void read(std::vector<const std::type_info*>& types, char delimiter, int start, int & end,
                  ConsumerProducerQueue<DataFrame*>& queue_out, ConsumerProducerQueue<std::string>& queue_in,
                  bool read_in_blocks, int blocksize) =0;};


class FileReader : public Reader {  // Declaration of the derived class CSVReader, inheriting from Reader
public:
    void read(std::vector<const std::type_info*>& types, char delimiter, int start, int & end,
              ConsumerProducerQueue<DataFrame*>& queue, ConsumerProducerQueue<std::string>& queue_in,
              bool read_in_blocks, int blocksize) override;
    // Implementation of the read function for reading CSV files
    // Parameters: filename - name of the CSV file to read from
    //             types - vector of type_info pointers specifying the types of data to read
    //             delimiter - character used as a delimiter in the CSV file (default: ',')
    //             queue - reference to the queue to store the data
    //             read_in_blocks - flag to indicate whether to read data in blocks or not (default: true)
};
/*
class DbReader : public Reader {  // Declaration of the derived class DbReader, inheriting from Reader
public:
    void readDb(const std::string& filename, ConsumerProducerQueue<DataFrame*>& queue, bool read_in_blocks,
                int blocksize, std::vector<const std::type_info*>& types);
    // reads the data from a db file with sqlite3
    void read(const std::string& filename, std::vector<const std::type_info*>& types, char delimiter, int start, int & end,
              ConsumerProducerQueue<DataFrame*>& queue, bool read_in_blocks, int blocksize) override;
};
*/
#endif // READER_H               // End of header guard