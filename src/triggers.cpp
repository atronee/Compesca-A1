#include <iostream>
#include <chrono>
#include <thread>
#include <filesystem>
#include "triggers.h"
#include "ConsumerProducerQueue.h"
#include <set>
#include <sys/file.h> // for flock

void TimeBasedTrigger::startTrigger(int seconds, VoidFunctionPtr executePipeline) {
    const auto interval = std::chrono::seconds(seconds);
    while (true) {
        executePipeline();
        std::this_thread::sleep_for(interval);
    }
}


bool EventBasedTrigger::isNewLogFile(const std::filesystem::path& folder, const std::string& filename) {
    // Check if the file is not in the set of processed files
    return processedFiles.find(filename) == processedFiles.end();
}

void EventBasedTrigger::triggerOnApperanceOfNewLogFile(const std::filesystem::path logFolder, ConsumerProducerQueue<std::string>& queue_files) {
    while (true) {
        if (std::filesystem::is_directory(logFolder)) {
            // Iterate through files in the log folder
            for (const auto& entry : std::filesystem::directory_iterator(logFolder)) {
                // check if file is not in the set of processed files and is a txt file
                // Open the file
                int fd = open(entry.path().c_str(), O_RDONLY);
                if (fd == -1) {
                    const auto interval = std::chrono::milliseconds (10);
                    std::this_thread::sleep_for(interval);
                    continue;
                }


                if ((entry.path().extension() == ".txt" || entry.path().extension() == ".csv")&& isNewLogFile(logFolder, entry.path().filename().string())){

                    // Process the new log file
                    processedFiles.insert(entry.path().filename().string());
                    queue_files.push(entry.path().string());
                    // Release the lock and close the file

                }

            }
        } else {
            std::cerr << "Error: " << logFolder << " is not a directory.\n";
            break;
        }

//        queue_files.push("STOP");
//        break;
        const auto interval = std::chrono::milliseconds (100);
        std::this_thread::sleep_for(interval);
    }
}