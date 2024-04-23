#ifndef COMPESCA_A1_MOCK_H
#define COMPESCA_A1_MOCK_H

#include <iostream>
#include <string>
#include "ConsumerProducerQueue.h"

int getRandomNormalizedInt(int, int);

int getRandomInt(int, int);

std::string getRandomNowBasedDate(int, int, int, int, int, int);

std::string getRandomDate(int, int, int, int, int, int, int, int, int, int, int, int);

std::string getRandomString(int);

std::string consumerData(bool);
std::string productData();
std::string stockData(int);
std::string orderData();

void mockCSV(const int numRecords = 1000);

std::string generateLogAudit();
std::string generateLogUserBehavior();
std::string generateLogFailureNotification();
std::string generateLogDebug();

enum LogType {
    LOG_AUDIT,
    LOG_USER_BEHAVIOR,
    LOG_FAILURE_NOTIFICATION,
    LOG_DEBUG
};

void writeColumnNames(std::ofstream&, LogType);
void mockLogFiles(int, int, int);

std::string createConsumerTable();
void mockSqliteTable(int);

void mockRandomRequest(ConsumerProducerQueue<std::string>&, int);

#endif //COMPESCA_A1_MOCK_H
