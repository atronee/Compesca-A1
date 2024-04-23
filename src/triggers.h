#ifndef COMPESCA_A1_TRIGGERS_H
#define COMPESCA_A1_TRIGGERS_H

#include <iostream>
#include <chrono>
#include <filesystem>
#include <set>
#include "ConsumerProducerQueue.h"

typedef void (*VoidFunctionPtr)();

class Trigger {
        public:
        // list of processed files
        std::set<std::string> processedFiles;
};

class TimeBasedTrigger : public Trigger {
public:
    void startTrigger(int seconds, VoidFunctionPtr executePipeline);
};

class EventBasedTrigger : public Trigger {
public:

    bool isNewLogFile(const std::filesystem::path& folder, const std::string& filename);

    void triggerOnApperanceOfNewLogFile(const std::filesystem::path logFolder,
                                        ConsumerProducerQueue<std::string>& queue_files);

};

#endif //COMPESCA_A1_TRIGGERS_H
