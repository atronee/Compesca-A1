from __future__ import print_function

import logging
import random 

import grpc
import contract_pb2
import contract_pb2_grpc

import mock


def guide_log_message(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.generateLogUserBehavior())
        log_message.append(contract_pb2.LogMessage(user_author_id=message["user_author_id"],
                                                   action=message["action"],
                                                   button_product_id=message["button_product_id"],
                                                   stimulus=message["stimulus"],
                                                   component=message["component"],
                                                   text_content=message["text_content"],
                                                   date=str(message["date"])))


    response = stub.LogUserBehavior(iter(log_message))
    print("LogMessage client received: " + response.response)


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = contract_pb2_grpc.LogServiceStub(channel)
        guide_log_message(stub, 10)

if __name__ == "__main__":
    logging.basicConfig()
    run()
