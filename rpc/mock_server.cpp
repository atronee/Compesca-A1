#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include "contract.grpc.pb.h"

#include "../src/ConsumerProducerQueue.h"
#include "../src/DataFrame.h"

using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using namespace data_service;

class MockServer final : public DataService::Service {
public:
    Status LogUserBehavior(ServerContext* context, ServerReader<LogUserBehaviorMessage>* reader,
                           Response* response, Consumer) override {
        LogUserBehaviorMessage message;
        while (reader->Read(&message)) {
            std::cout << "Received logged user behavior: "<< std::endl;
            std::cout << "User ID: " << message.user_author_id() << std::endl;
            std::cout << "User action: " << message.action() << std::endl;
            std::cout << "Button product id: " << message.button_product_id() << std::endl;
            std::cout << "Stimulus" << message.stimulus() << std::endl;
            std::cout << "Text content: " << message.text_content() << std::endl;
            std::cout << "Date: " << message.date() << std::endl;
            std::cout << "--------------------------------------------" << std::endl;
        }
        response->set_response("User behavior logged successfully");
        return Status::OK;
    }

    Status LogAudit (ServerContext* context, ServerReader<LogAuditMessage>* reader,
                     Response* response) override {
        LogAuditMessage message;
        while (reader->Read(&message)) {
            std::cout << "Received audit log: "<< std::endl;
            std::cout << "User ID: " << message.user_author_id() << std::endl;
            std::cout << "Action: " << message.action() << std::endl;
            std::cout << "Date: " << message.date() << std::endl;
            std::cout << "--------------------------------------------" << std::endl;
        }
        response->set_response("Audit log received successfully");
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
