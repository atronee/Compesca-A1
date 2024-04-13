#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdlib> // For rand() and srand()
#include <ctime>   // For time()
#include <filesystem>
#include "mock.h"

// Function to generate a random integer within a range
int getRandomInt(int min, int max) {
    return min + rand() % (max - min + 1);
}

// Function to generate a random string of specified length
/*
std::string getRandomString(int length) {
    const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string result;
    for (int i = 0; i < length; ++i) {
        result.push_back(charset[getRandomInt(0, charset.size() - 1)]);
    }
    return result;
}
*/

// Function to generate random mock consumer data
std::string generateMockConsumerData() {
    const std::vector<std::string> names = {"Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace"};
    const std::vector<std::string> products = {"Laptop", "Smartphone", "Headphones", "Camera", "Fitness Tracker"};
    const std::vector<std::string> cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"};
    const std::vector<std::string> shops = {"TechStore", "GadgetHub", "ElectroMall", "OnlineGizmos"};
    const std::vector<std::string> status_states = {"visualized", "purchased"};

    std::string name = names[getRandomInt(0, names.size() - 1)];
    std::string product = products[getRandomInt(0, products.size() - 1)];
    std::string shop = shops[getRandomInt(0, shops.size() - 1)];
    std::string city = cities[getRandomInt(0, cities.size() - 1)];
    std::string status = status_states[getRandomInt(0, status_states.size() - 1)];

    int day = getRandomInt(1, 31);
    int month = getRandomInt(1, 12);
    int year = getRandomInt(2019, 2024);
    int hour = getRandomInt(0, 23);
    int minute = getRandomInt(0, 9);
    int tenMinute = getRandomInt(0, 5);
    int tenSecond = getRandomInt(0, 5);
    int second = getRandomInt(0, 9);

    return name + "," + product + "," + shop + "," + city + "," + status + "," +
            std::to_string(year) + "/" + std::to_string(month)+ "/" + std::to_string(day) + " " +
           std::to_string(hour) + ":" + std::to_string(minute) + std::to_string(tenMinute) + ":" +
           std::to_string(tenSecond) + std::to_string(second);
}

void mockCSV()
{
    // Seed the random number generator
    srand(static_cast<unsigned int>(time(nullptr)));

    // Number of mock data records to generate
    const int numRecords = 10;

    // Output file name
    const std::string outputFileName = "mock_consumer_data.csv";

    // Open the output file
    std::ofstream outputFile(outputFileName);
    if (!outputFile.is_open()) {
        std::cerr << "Error opening output file.\n";
        return;
    }

    //column names
    outputFile << "Name,Product,Shop,City,Status,Date\n";
    // Generate and write mock consumer data to the file
    for (int i = 0; i < numRecords; ++i) {
        std::string consumerData = generateMockConsumerData();
        outputFile << consumerData << "\n";
    }

    // Close the output file
    outputFile.close();

    std::cout << "Mock consumer data generation complete. Check " << outputFileName << " for the generated data.\n";
}

// Mocks the behavior of a continuous status machine that constantly creates log files
void mockLogFiles()
{
    srand(static_cast<unsigned int>(time(nullptr)));

    // Output directory path
    const std::string outputDir = "logs/";

    // Create the output directory if it doesn't exist
    system(("mkdir -p " + outputDir).c_str());

    // Number of log lines per file
    const int linesPerFile = 100;

    // Number of log files to generate
    const int numFiles = 5;

    for (int fileIndex = 1; fileIndex <= numFiles; ++fileIndex) {
        std::string filename = outputDir + "log_" + std::to_string(fileIndex) + ".txt";

        // Open the output file
        std::ofstream outputFile(filename);
        if (!outputFile.is_open()) {
            std::cerr << "Error opening output file: " << filename << "\n";
            return;
        }

        for (int line = 1; line <= linesPerFile; ++line) {
            std::string logMessage = generateMockConsumerData();
            outputFile << logMessage << "\n";
        }

        // Close the output file
        outputFile.close();

        std::cout << "Log file generated: " << filename << "\n";
    }

    std::cout << "Mock log generation complete.\n";
}

int main() {
    //mockCSV();
    mockLogFiles();
    return 0;
}