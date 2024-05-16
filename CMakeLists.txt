cmake_minimum_required(VERSION 3.15)
project(Compesca_A1)

set(CMAKE_CXX_STANDARD 20)
set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "Propagate CXX_STANDARD from parent project")

# Find required protobuf package
option(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
if(CMAKE_CROSSCOMPILING)
    find_program(_PROTOBUF_PROTOC protoc)
else()
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
if(CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

include_directories(${PROTOBUF_INCLUDE_DIR})

# Proto file
get_filename_component(rg_proto "rpc/contract.proto" ABSOLUTE)
get_filename_component(rg_proto_path "${rg_proto}" PATH)

# Generated sources
set(rg_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/contract.pb.cc")
set(rg_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/contract.pb.h")
set(rg_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/contract.grpc.pb.cc")
set(rg_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/contract.grpc.pb.h")
add_custom_command(
        OUTPUT "${rg_proto_srcs}" "${rg_proto_hdrs}" "${rg_grpc_srcs}" "${rg_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${rg_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${rg_proto}"
        DEPENDS "${rg_proto}")

# Include generated directories
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# rg_grpc_proto
add_library(rg_grpc_proto
        ${rg_grpc_srcs}
        ${rg_grpc_hdrs}
        ${rg_proto_srcs}
        ${rg_proto_hdrs})
target_link_libraries(rg_grpc_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})

add_executable(mock_server rpc/mock_server.cpp)
target_link_libraries(mock_server
        rg_grpc_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})

find_package(SQLite3)
add_executable(Compesca_A1
        src/mock.cpp
        src/mock.h
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
        dashboardCalc.cpp
        src/DataFrameVersionManager.cpp
        src/DataFrameVersionManager.h
        src/writer.cpp
        src/writer.h
)
target_link_libraries(Compesca_A1 SQLite::SQLite3)


