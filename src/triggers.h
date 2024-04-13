#ifndef COMPESCA_A1_TRIGGERS_H
#define COMPESCA_A1_TRIGGERS_H

#include <iostream>
#include <chrono>
#include <filesystem>

class Trigger {
        public:
        virtual void executePipeline() = 0; // Pure virtual function for pipeline execution
};

class TimeBasedTrigger : public Trigger {
public:
    void executePipeline() override;

    void startTrigger(const std::chrono::seconds &interval);
};

class EventBasedTrigger : public Trigger {
public:
    void executePipeline() override;

    void triggerOnApperanceOfNewLogFile(const std::filesystem::path logFolder);
};

#endif //COMPESCA_A1_TRIGGERS_H
