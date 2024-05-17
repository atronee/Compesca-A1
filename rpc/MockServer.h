#ifndef COMPESCA_A1_MOCKSERVER_H
#define COMPESCA_A1_MOCKSERVER_H

#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include "contract.grpc.pb.h"
#include "../src/ConsumerProducerQueue.h"
#include "../src/DataFrame.h"
#include <vector>
#include <memory>  // For std::unique_ptr

using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using namespace data_service;




class MockServer final : public DataService::Service {
private:
    int max_dataFrame_size;
public:
    explicit MockServer(int max_dataFrame_size=100) : max_dataFrame_size(max_dataFrame_size) {};
    ConsumerProducerQueue<DataFrame> queueLogAudit;
    ConsumerProducerQueue<DataFrame> queueLogUserBehavior;
    ConsumerProducerQueue<DataFrame> queueLogFailureNotification;
    ConsumerProducerQueue<DataFrame> queueLogDebug;
    ConsumerProducerQueue<DataFrame> queueConsumerData;
    ConsumerProducerQueue<DataFrame> queueProductData;
    ConsumerProducerQueue<DataFrame> queueStockData;
    ConsumerProducerQueue<DataFrame> queueOrderData;

    // All the methods of this class behave the same way, they transform a stream of messages into a DataFrame and push it to a queue.
    // The only difference is the type of message they receive.
    Status LogUserBehavior(ServerContext* context, ServerReader<LogUserBehaviorMessage>* reader,
                           Response* response) override;

    Status LogAudit (ServerContext* context, ServerReader<LogAuditMessage>* reader,
                     Response* response) override;

    Status LogFailureNotification(ServerContext* context, ServerReader<LogFailureNotificationMessage>* reader,
                                  Response* response) override;

    Status LogDebug(ServerContext* context, ServerReader<LogDebugMessage>* reader,
                    Response* response) override;

    Status ConsumerData(ServerContext* context, ServerReader<ConsumerDataMessage>* reader,
                        Response* response) override;

    Status ProductData(ServerContext* context, ServerReader<ProductDataMessage>* reader,
                       Response* response) override;

    Status StockData(ServerContext* context, ServerReader<StockDataMessage>* reader,
                     Response* response) override;

    Status OrderData(ServerContext* context, ServerReader<OrderDataMessage>* reader,
                     Response* response) override;

};

void RunServer(MockServer& service);

void distributeDataFrames(
        ConsumerProducerQueue<DataFrame>& sourceQueue,
        std::vector<std::unique_ptr<ConsumerProducerQueue<DataFrame *>>>& destinationQueues);

#endif //COMPESCA_A1_MOCKSERVER_H
