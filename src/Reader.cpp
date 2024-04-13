#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <any>
#include <chrono>
#include <stdexcept>

class Reader {
public:
    virtual std::pair<std::vector<std::string>, std::unordered_map<std::string, std::vector<std::any>>> read(const std::string& filename, const std::map<std::string, std::string>& types, char delimiter = ',') = 0;
};

class CSVReader : public Reader {
public:
    std::pair<std::vector<std::string>, std::unordered_map<std::string, std::vector<std::any>>> read(const std::string& filename, const std::map<std::string, std::string>& types, char delimiter = ',') override {
        std::ifstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Error opening CSV file: " << filename << std::endl;
            return {};
        }

        std::vector<std::string> column_order;
        std::unordered_map<std::string, std::vector<std::any>> data;
        
        std::string line;
        bool header = true;
        while (std::getline(file, line)) {
            std::vector<std::any> row_data;
            std::vector<std::string> row;
            std::stringstream ss(line);
            std::string cell;
            while (std::getline(ss, cell, delimiter)) {
                row.push_back(cell);
            }
            if (header) {
                for (const auto& col : row) {
                    column_order.push_back(col);
                    data[col] = {};
                }
                header = false;
            } else {
                for (size_t i = 0; i < row.size(); ++i) {
                    if (types.find(column_order[i]) != types.end()) {
                        // Add more types as needed
                        if (types.at(column_order[i]) == "int") {
                            row_data.push_back(std::stoi(row[i]));
                        } else if (types.at(column_order[i]) == "float") {
                            row_data.push_back(std::stof(row[i]));
                        } else if (types.at(column_order[i]) == "date") {
                            // TODO: date parsing
                            row_data.push_back(row[i]);
                        } else {
                            row_data.push_back(row[i]);
                        }
                    } else {
                        row_data.push_back(row[i]);
                    }
                }
                for (size_t i = 0; i < row_data.size(); ++i) {
                    data[column_order[i]].push_back(row_data[i]);
                }
            }
        }

        file.close();
        return {column_order, data};
    }
};

int main() {
    CSVReader csvReader;

    std::map<std::string, std::string> types = {
        {"Nome", "string"},
        {"Idade", "int"},
        {"Cidade", "string"},
        {"Data", "string"}
    };

    auto [column_order, data] = csvReader.read("testDataFrame/data.csv", types);


    return 0;
}
