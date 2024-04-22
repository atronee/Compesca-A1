//
// Created by vini on 22/04/24.
//

#ifndef COMPESCA_A1_WRITER_H
#define COMPESCA_A1_WRITER_H
#include "DataFrame.h"

void write_to_sqlite(DataFrame* fileDF, const std::string& filePath, const std::string& table, bool deleteFlag = false);

#endif //COMPESCA_A1_WRITER_H
