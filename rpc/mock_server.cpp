#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include "contract.grpc.pb.h"

using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using log_service::LogService;
using log_service::LogMessage;
using log_service::LogResponse;

class MockServer final : public LogService::Service {
public:
    Status LogUserBehavior(grpc::ServerContext* context, ServerReader<LogMessage>* reader,
                           LogResponse* response) override {
        LogMessage message;
        while (reader->Read(&message)) {
            std::cout << "Received logged user behavior: "<< std::endl;
            std::cout << "User ID: " << message.user_author_id() << std::endl;
            std::cout << "User action: " << message.action() << std::endl;
            std::cout << "Button product id: " << message.button_product_id() << std::endl;
            std::cout << "Stimulus" << message.stimulus() << std::endl;
            std::cout << "Text content: " << message.text_content() << std::endl;
            std::cout << "Date: " << message.date() << std::endl;
        }
        response->set_response("User behavior logged successfully");
        return Status::OK;
    }
};

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    MockServer service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main() {
    RunServer();

    return 0;
}
