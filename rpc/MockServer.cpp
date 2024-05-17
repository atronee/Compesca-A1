#include "MockServer.h"
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <string>
#include <vector>
#include <typeinfo>
#include "contract.grpc.pb.h"

#include "../src/ConsumerProducerQueue.h"
#include "../src/DataFrame.h"

using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using std::string;
using namespace data_service;


Status MockServer::LogAudit (ServerContext* context, ServerReader<LogAuditMessage>* reader,
                 Response* response) {
    LogAuditMessage message;
    auto types = std::vector< const std::type_info*>(
            {&typeid(int), &typeid(string), &typeid(string), &typeid(string), &typeid(string)});
    auto names = std::vector<string>({"user_author_id", "action", "action_description","text_content", "date"});

    DataFrame dataFrame(names,types);

    int i = 0;
    while (reader->Read(&message)) {
        dataFrame.add_row({message.user_author_id(), message.action(),
                           message.action_description(), message.text_content(), message.date()});
        i++;
        if (i == max_dataFrame_size) {
            queueLogAudit.push(dataFrame);
            i = 0;
            dataFrame = DataFrame(names,types);
        }
    }
    if (i > 0) {
        queueLogAudit.push(dataFrame);
    }
    response->set_response("Audit log received successfully");
    return Status::OK;
}

Status MockServer::LogUserBehavior(ServerContext* context, ServerReader<LogUserBehaviorMessage>* reader,
                                   Response* response) {
    LogUserBehaviorMessage message;
    auto types = std::vector< const std::type_info*>(
            {&typeid(int), &typeid(string), &typeid(int), &typeid(string),
             &typeid(string), &typeid(string), &typeid(string)});
    auto names = std::vector<string>({"user_author_id", "action", "button_product_id",
                                      "stimulus", "component", "text_content", "date"});

    DataFrame dataFrame(names,types);

    int i = 0;
    while (reader->Read(&message)) {
        dataFrame.add_row({message.user_author_id(), message.action(), message.button_product_id(),
                           message.stimulus(), message.text_content(), message.date()});
        i++;
        if (i == max_dataFrame_size) {
            queueLogUserBehavior.push(dataFrame);
            i = 0;
            dataFrame = DataFrame(names,types);
        }
    }
    if (i > 0) {
        queueLogUserBehavior.push(dataFrame);
    }
    response->set_response("User behavior logged successfully");
    return Status::OK;
}

Status MockServer::LogFailureNotification(::grpc::ServerContext *context,
                                          ::grpc::ServerReader<::data_service::LogFailureNotificationMessage> *reader,
                                          ::data_service::Response *response) {
    LogFailureNotificationMessage message;
    auto types = std::vector< const std::type_info*>(
            {&typeid(string), &typeid(string), &typeid(string), &typeid(string), &typeid(string)});
    auto names = std::vector<string>({"component", "severity", "message", "text_content", "date"});

    DataFrame dataFrame(names,types);

    int i = 0;
    while (reader->Read(&message)) {
        dataFrame.add_row({message.component(), message.severity(), message.message(),
                           message.text_content(), message.date()});
        i++;
        if (i == max_dataFrame_size) {
            queueLogFailureNotification.push(dataFrame);
            i = 0;
            dataFrame = DataFrame(names,types);
        }
    }
    if (i > 0) {
        queueLogFailureNotification.push(dataFrame);
    }
    response->set_response("Failure notification logged successfully");

    return Status::OK;
}

Status MockServer::LogDebug(grpc::ServerContext *context, ServerReader<data_service::LogDebugMessage> *reader,
                            data_service::Response *response) {
    LogDebugMessage message;
    auto types = std::vector< const std::type_info*>(
            {&typeid(string), &typeid(string), &typeid(string)});
    auto names = std::vector<string>({"message", "text_content","date"});

    DataFrame dataFrame(names,types);

    int i = 0;
    while (reader->Read(&message)) {
        dataFrame.add_row({message.message(), message.text_content(), message.date()});
        i++;
        if (i == max_dataFrame_size) {
            queueLogDebug.push(dataFrame);
            i = 0;
            dataFrame = DataFrame(names,types);
        }
    }
    if (i > 0) {
        queueLogDebug.push(dataFrame);
    }
    response->set_response("Debug log received successfully");

    return Status::OK;
}

Status MockServer::ConsumerData(grpc::ServerContext *context, grpc::ServerReader<data_service::ConsumerDataMessage> *reader,
                                data_service::Response *response) {
    ConsumerDataMessage message;
    auto types = std::vector< const std::type_info*>(
            {&typeid(int), &typeid(string), &typeid(string), &typeid(string), &typeid(string), &typeid(string)});
    auto names = std::vector<string>({"user_id", "name", "surname", "city", "born_date", "register_date"});

    DataFrame dataFrame(names,types);

    int i = 0;
    while (reader->Read(&message)) {
        dataFrame.add_row({message.user_id(), message.name(), message.surname(),
                           message.city(), message.born_date(), message.register_date()});
        i++;
        if (i == max_dataFrame_size) {
            queueConsumerData.push(dataFrame);
            i = 0;
            dataFrame = DataFrame(names,types);
        }
    }
    if (i > 0) {
        queueConsumerData.push(dataFrame);
    }
    response->set_response("Consumer data logged successfully");

    return Status::OK;
}

Status MockServer::ProductData(grpc::ServerContext *context, grpc::ServerReader<data_service::ProductDataMessage> *reader,
                               data_service::Response *response) {
    ProductDataMessage message;
    auto types = std::vector< const std::type_info*>(
            {&typeid(int), &typeid(string), &typeid(string), &typeid(int), &typeid(string),});
    auto names = std::vector<string>({"product_id", "name", "image", "price", "description"});

    DataFrame dataFrame(names,types);

    int i = 0;
    while (reader->Read(&message)) {
        dataFrame.add_row({message.product_id(), message.name(), message.image(),
                           message.price(), message.description()});
        i++;
        if (i == max_dataFrame_size) {
            queueProductData.push(dataFrame);
            i = 0;
            dataFrame = DataFrame(names,types);
        }
    }
    if (i > 0) {
        queueProductData.push(dataFrame);
    }
    response->set_response("Product data logged successfully");

    return Status::OK;
}

Status MockServer::StockData(grpc::ServerContext *context, ServerReader<data_service::StockDataMessage> *reader,
                             data_service::Response *response) {
    StockDataMessage message;
    auto types = std::vector< const std::type_info*>(
            {&typeid(int), &typeid(int)});
    auto names = std::vector<string>({"product_id", "quantity"});

    DataFrame dataFrame(names,types);

    int i = 0;
    while (reader->Read(&message)) {
        dataFrame.add_row({message.product_id(), message.quantity()});
        i++;
        if (i == max_dataFrame_size) {
            queueStockData.push(dataFrame);
            i = 0;
            dataFrame = DataFrame(names,types);
        }
    }
    if (i > 0) {
        queueStockData.push(dataFrame);
    }
    response->set_response("Stock data logged successfully");

    return Status::OK;
}

Status MockServer::OrderData(grpc::ServerContext *context, ServerReader<data_service::OrderDataMessage> *reader,
                             data_service::Response *response) {
    OrderDataMessage message;
    auto types = std::vector< const std::type_info*>({&typeid(int), &typeid(int), &typeid(int), &typeid(string),
                                                      &typeid(string), &typeid(string), &typeid(string)});
    auto names = std::vector<string>({"user_id", "product_id", "quantity", "purchase_date", "payment_date",
                                      "shipping_date", "delivery_date"});

    DataFrame dataFrame(names,types);

    int i = 0;
    while (reader->Read(&message)) {
        dataFrame.add_row({message.user_id(), message.product_id(), message.quantity(), message.purchase_date(),
                           message.payment_date(), message.shipping_date(), message.delivery_date()});
        i++;
        if (i == max_dataFrame_size) {
            queueOrderData.push(dataFrame);
            i = 0;
            dataFrame = DataFrame(names,types);
        }
    }
    if (i > 0) {
        queueOrderData.push(dataFrame);
    }
    response->set_response("Order data logged successfully");

    return Status::OK;
}

void RunServer(MockServer& service) {

    std::string server_address("0.0.0.0:50051");

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

void distributeDataFrames(
        ConsumerProducerQueue<DataFrame>& sourceQueue,
        std::vector<std::unique_ptr<ConsumerProducerQueue<DataFrame*>>>& destinationQueues)
{
    // This function is a thread that pops DataFrames from a source queue and pushes them to multiple destination queues.
    //The source queue comes from the gRPC server.
    while (true) {
        DataFrame df = sourceQueue.pop();  // Blocks if the queue is empty, and pops a DataFrame object

        // Make a copy of the DataFrame and push it to each destination queue
        for (auto& queue : destinationQueues) {
            auto dfCopy = std::make_unique<DataFrame>(df);  // Deep copy the DataFrame
            // Push the copy as a pointer to the destination queue
            queue->push(dfCopy.release());
        }
    }
}