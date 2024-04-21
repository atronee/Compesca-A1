#include <iostream>
#include <chrono>
#include <thread>
#include <filesystem>
#include "triggers.h"
#include "ConsumerProducerQueue.h"
#include <set>


// Child class TimeBasedTrigger
void TimeBasedTrigger::executePipeline(){
// Your time-based pipeline execution logic here
    std::cout << "Time-based trigger: Pipeline executed\n";
}
void TimeBasedTrigger::startTrigger(int seconds) {
    const auto interval = std::chrono::seconds(seconds);
    while (true) {
        executePipeline();
        std::this_thread::sleep_for(interval);
    }
}

// Child class EventBasedTrigger
void EventBasedTrigger::executePipeline(){
    // Your event-based pipeline execution logic here
    std::cout << "Event-based trigger: Pipeline executed\n";
}

bool EventBasedTrigger::isNewLogFile(const std::filesystem::path& folder, const std::string& filename) {
    // Check if the file is not in the set of processed files
    return processedFiles.find(filename) == processedFiles.end();
}

void EventBasedTrigger::triggerOnApperanceOfNewLogFile(const std::filesystem::path logFolder, ConsumerProducerQueue<std::string>& queue_files) {
    while (true) {
        // Check for new log files every 5 seconds
        const auto interval = std::chrono::seconds(5);
        std::this_thread::sleep_for(interval);

        // Iterate through files in the log folder
        for (const auto& entry : std::filesystem::directory_iterator(logFolder)) {
            // check if file is not in the set of processed files and is a txt file
            if (entry.path().extension() == ".txt" && isNewLogFile(logFolder, entry.path().filename().string())){
                // Process the new log file
                std::cout << "New log file detected: " << entry.path().filename() << std::endl;
                processedFiles.insert(entry.path().filename().string());
                queue_files.push(entry.path().string());
            }
        }
    }
}