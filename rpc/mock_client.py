from __future__ import print_function

import logging
import random 

import grpc
import contract_pb2
import contract_pb2_grpc

import mock


def guide_log_message(stub, user_author_id, action, button_product_id, stimulus, component, text_content, date):
    log_message = contract_pb2.LogMessage(user_author_id=user_author_id, action=action, button_product_id=button_product_id, stimulus=stimulus, component=component, text_content=text_content, date=date)
    response = stub.LogUserBehavior(iter([log_message]))
    print("LogMessage client received: " + response.response)


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = contract_pb2_grpc.LogServiceStub(channel)
        for _ in range(10):
            log_message = mock.generateLogUserBehavior()
            user_author_id = int(log_message["user_author_id"])
            action = log_message["action"]
            button_product_id = int(log_message["button_product_id"])
            stimulus = log_message["stimulus"]
            component = log_message["component"]
            text_content = log_message["text_content"]
            date = str(log_message["date"])

            guide_log_message(stub, user_author_id, action,
                            button_product_id, stimulus,
                            component, text_content, date)
            

if __name__ == "__main__":
    logging.basicConfig()
    run()
