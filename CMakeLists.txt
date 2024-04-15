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
target_link_libraries(TestDataframe)

add_executable(TestReader
        test_reader.cpp
        src/DataFrame.h
        src/DataFrame.cpp
        src/Reader.h
        src/Reader.cpp
        src/Semaphore.cpp
        src/Semaphore.cpp
        src/ConsumerProducerQueue.h)
target_link_libraries(TestReader)

# test mock
add_executable(test_mock
        src/mock.h
        src/mock.cpp
)

# test trigger
add_executable(test_trigger
        src/triggers.cpp
        src/triggers.h
)