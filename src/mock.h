#ifndef COMPESCA_A1_MOCK_H
#define COMPESCA_A1_MOCK_H

#include <iostream>
#include <string>

int getRandomInt(int, int);

std::string getRandomString(int);

std::string consumerData(bool);
std::string productData();
std::string stockData();
std::string orderData();

void mockCSV(int);

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
void mockLogFiles(int, int);

std::string createConsumerTable();
void mockSqliteTable(int);

#endif //COMPESCA_A1_MOCK_H
