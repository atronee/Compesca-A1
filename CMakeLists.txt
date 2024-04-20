cmake_minimum_required(VERSION 3.27)
project(Compesca_A1)

set(CMAKE_CXX_STANDARD 20)

add_executable(TestDataframe
        test_dataframe.cpp
        src/DataFrame.h
        src/DataFrame.cpp
        src/Semaphore.cpp
        src/Semaphore.cpp
        src/ConsumerProducerQueue.h
)


add_executable(TestReader
        test_reader.cpp
        src/DataFrame.h
        src/DataFrame.cpp
        src/Reader.h
        src/Reader.cpp
        src/Semaphore.cpp
        src/Semaphore.cpp
        src/ConsumerProducerQueue.h)


add_executable(TestPipeline
        test_pipeline.cpp
        src/DataFrame.h
        src/DataFrame.cpp
        src/Reader.h
        src/Reader.cpp
        src/Semaphore.h
        src/Semaphore.cpp
        src/ConsumerProducerQueue.h
        src/Handlers.h
        src/Handlers.cpp)

# test mock
find_package(SQLite3)
add_executable(test_mock
        src/mock.h
        src/mock.cpp
)
target_link_libraries(test_mock SQLite::SQLite3)

# test trigger
add_executable(test_trigger
        src/triggers.cpp
        src/triggers.h
        sqlite3.h
)




