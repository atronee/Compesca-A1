//
// Implementation idea of DataFrame and data reader
// Initially only for CSV
// We need to pass the file path, column types (should it do this automatically?), and the CSV delimiter.
//


#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <map>
#include <ctime> 
#include <iomanip> 

// Interface for DataFrame
class DataFrame {
public:
    virtual void print() const = 0; // virtual method to print the data
    virtual void info() const = 0;  // virtual method to display information about the data
};

// Class for CSV file reading, implementing the DataFrame interface
class CSVReader : public DataFrame {
private:
    std::vector<std::vector<std::string>> data;  // Stores the CSV data
    std::map<std::string, std::vector<std::string>> columns;  // Maps column names to values
    std::map<std::string, std::string> columnTypes;  // Maps column names to data types

    // Private method to read the CSV file
    void readCSV(const std::string& filename, const std::map<std::string, std::string>& types, char delimiter = ',') {
        std::ifstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Error opening CSV file: " << filename << std::endl;
            return;
        }

        std::string line;
        bool header = true;
        while (std::getline(file, line)) {
            std::vector<std::string> row;
            std::stringstream ss(line);
            std::string cell;
            while (std::getline(ss, cell, delimiter)) {
                row.push_back(cell);
            }
            if (header) {
                for (const auto& col : row) {
                    columns[col] = {};
                    if (types.find(col) != types.end()) {
                        columnTypes[col] = types.at(col); // Assign type for each column
                    } else {
                        // Assuming default type is string if not specified
                        columnTypes[col] = "string";
                    }
                }
                header = false;
            } else {
                for (size_t i = 0; i < row.size(); ++i) {
                    columns[data[0][i]].push_back(row[i]);
                }
            }
            data.push_back(row);
        }

        file.close();
    }

public:
    // Constructor that reads the CSV file and initializes column types
    CSVReader(const std::string& filename, const std::map<std::string, std::string>& types, char delimiter = ',') {
        readCSV(filename, types, delimiter);
    }

    // Method to print the CSV data
    void print() const override {
        std::cout << "CSV file data:" << std::endl;
        for (const auto& row : data) {
            for (const auto& cell : row) {
                std::cout << cell << "\t";
            }
            std::cout << std::endl;
        }
    }

    // Method to display information about the CSV data
    void info() const override {
        std::cout << "CSV file info:" << std::endl;
        for (const auto& column : columns) {
            std::cout << "Column: " << column.first << std::endl;
            std::cout << "Data type: " << columnTypes.at(column.first) << std::endl;
            std::cout << "Number of instances: " << column.second.size() << std::endl;
            if (columnTypes.at(column.first) == "date") {
                std::cout << "Example date: " << column.second[0] << std::endl;
            }
            std::cout << std::endl;
        }
    }
};

// Class for a data repository
class DataRepository {
private:
    DataFrame* dataFrame;

public:
    // Constructor accepting a pointer to DataFrame
    DataRepository(DataFrame* df) : dataFrame(df) {}

    // Method to print the DataFrame data
    void printDataFrame() const {
        dataFrame->print();
    }

    // Method to print information about the DataFrame data
    void printInfo() const {
        dataFrame->info();
    }
};

// Main function
int main() {
    // Define data types for each column
    std::map<std::string, std::string> columnTypes = {
            {"Name", "string"},
            {"Age", "int"},
            {"City", "string"},
            {"Date", "date"}
        };  

    // Create a CSV reader and read the CSV file
    CSVReader csvReader("data.csv", columnTypes, ','); // Using comma as delimiter
    // TXTReader txtReader("data.txt");

    // Create a data repository and pass the CSV reader to it
    DataRepository csvRepository(&csvReader);
    // DataRepository txtRepository(&txtReader);

    // Print the CSV data and info
    csvRepository.printDataFrame();
    csvRepository.printInfo();
    // txtRepository.printInfo();

    return 0;
}
