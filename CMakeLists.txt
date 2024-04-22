cmake_minimum_required(VERSION 3.27)
project(Compesca_A1)

set(CMAKE_CXX_STANDARD 20)



find_package(SQLite3)
add_executable(Compesca_A1
        dashboardCalc.cpp
        src/DataFrame.h
        src/DataFrame.cpp
        src/Reader.h
        src/Reader.cpp
        src/Semaphore.h
        src/Semaphore.cpp
        src/ConsumerProducerQueue.h
        src/Handlers.h
        src/Handlers.cpp
        src/triggers.h
        src/triggers.cpp
)
target_link_libraries(Compesca_A1 SQLite::SQLite3)
