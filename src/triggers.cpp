#include <iostream>
#include <chrono>
#include <thread>
#include <filesystem>
#include "triggers.h"

//Utility
bool isNewLogFile(const std::filesystem::path& folder, const std::string& filename) {
    // Check if the file exists in the folder and is not empty
    std::filesystem::path filePath = folder / filename;
    return std::filesystem::exists(filePath) && std::filesystem::file_size(filePath) > 0;
}
// Child class TimeBasedTrigger
void TimeBasedTrigger::executePipeline(){
// Your time-based pipeline execution logic here
    std::cout << "Time-based trigger: Pipeline executed\n";
}
void TimeBasedTrigger::startTrigger(const std::chrono::seconds& interval) {
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

void EventBasedTrigger::triggerOnApperanceOfNewLogFile(const std::filesystem::path logFolder) {
    while (true) {
        // Check for new log files every 5 seconds
        const auto interval = std::chrono::seconds(5);
        std::this_thread::sleep_for(interval);

        // Iterate through files in the log folder
        for (const auto& entry : std::filesystem::directory_iterator(logFolder)) {
            if (entry.is_regular_file() && isNewLogFile(logFolder, entry.path().filename())) {
                // Trigger pipeline execution for the new log file
                executePipeline();
            }
        }
    }
}
int main() {
    // Create instances of TimeBasedTrigger and EventBasedTrigger
    TimeBasedTrigger timeTrigger;
    EventBasedTrigger eventTrigger;

    // Start time-based trigger every 5 seconds
    // const auto interval = std::chrono::seconds(5);
    // timeTrigger.startTrigger(interval);

    // Trigger event-based pipeline execution
    eventTrigger.triggerOnApperanceOfNewLogFile("./");


    return 0;
}
