#include "src/triggers.h"
#include "src/ConsumerProducerQueue.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <vector>

void pipeline() {
    std::cout << "Pipeline executed" << std::endl;
}

int main()
{
    TimeBasedTrigger timeBasedTrigger;
    EventBasedTrigger eventBasedTrigger;
    ConsumerProducerQueue<std::string> queue;

    timeBasedTrigger.startTrigger(1, pipeline);  // executa a função passada a cada x segundos
    eventBasedTrigger.triggerOnApperanceOfNewLogFile("./logs", queue); // coloca o nome do arquivo novo da pasta observada na queue passada
    return 0;
}