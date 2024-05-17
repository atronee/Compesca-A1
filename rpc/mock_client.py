from __future__ import print_function

import logging
import random 
import grpc
import contract_pb2
import contract_pb2_grpc
import mock
import multiprocessing


def log_user_behavior_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.generateLogUserBehavior())
        log_message.append(contract_pb2.LogUserBehaviorMessage(user_author_id=message["user_author_id"],
                                                   action=message["action"],
                                                   button_product_id=message["button_product_id"],
                                                   stimulus=message["stimulus"],
                                                   component=message["component"],
                                                   text_content=message["text_content"],
                                                   date=str(message["date"])))


    response = stub.LogUserBehavior(iter(log_message))
    print("LogMessage client received: " + response.response)

def log_audit_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.generateLogAudit())
        log_message.append(contract_pb2.LogAuditMessage(user_author_id=message["user_author_id"],
                                                   action=message["action"],
                                                   action_description=message["action_description"],
                                                   text_content=message["text_content"],
                                                   date=str(message["date"])))

    response = stub.LogAudit(iter(log_message))
    print("LogMessage client received: " + response.response)

def log_failure_notification_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.generateLogFailureNotification())
        log_message.append(contract_pb2.LogFailureNotificationMessage(component=message["component"],
                                                   severity=message["severity"],
                                                   message=message["message"],
                                                   text_content=message["text_content"],
                                                   date=str(message["date"])))

    response = stub.LogFailureNotification(iter(log_message))
    print("LogMessage client received: " + response.response)

def log_debug_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.generateLogDebug())
        log_message.append(contract_pb2.LogDebugMessage(message=message["message"],
                                                   text_content=message["text_content"],
                                                   date=str(message["date"])))

    response = stub.LogDebug(iter(log_message))
    print("LogMessage client received: " + response.response)

def consumer_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.generateConsumer())
        log_message.append(contract_pb2.ConsumerMessage(user_id=message["user_id"],
                                                   name=message["name"],
                                                   surname=message["surname"],
                                                   city=message["city"],
                                                   born_date=str(message["born_date"]),
                                                   registration_date=str(message["registration_date"])))

    response = stub.Consumer(iter(log_message))
    print("LogMessage client received: " + response.response)

def order_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.generateOrder())
        log_message.append(contract_pb2.OrderMessage(user_id=message["user_id"],
                                                   product_id=message["product_id"],
                                                   quantity=message["quantity"],
                                                   purchase_date=str(message["purchase_date"]),
                                                   payment_date=str(message["payment_date"]),
                                                   shipping_date=str(message["shipping_date"]),
                                                   delivery_date=str(message["delivery_date"])))

    response = stub.Order(iter(log_message))
    print("LogMessage client received: " + response.response)

def product_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.generateProduct())
        log_message.append(contract_pb2.ProductMessage(product_id=message["product_id"],
                                                   product_name=message["name"],
                                                   image_url=message["image"],
                                                   price=message["price"],
                                                   description=message["description"]))

    response = stub.Product(iter(log_message))
    print("LogMessage client received: " + response.response)

def stock_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.generateStock())
        log_message.append(contract_pb2.StockMessage(product_id=message["product_id"],
                                                   quantity=message["quantity"]))

    response = stub.Stock(iter(log_message))
    print("LogMessage client received: " + response.response)

num_messages = 10
num_processes = 10

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = contract_pb2_grpc.DataServiceStub(channel)
        log_user_behavior_messages(stub, num_messages)
        log_audit_messages(stub, num_messages)
        log_failure_notification_messages(stub, num_messages)
        log_debug_messages(stub, num_messages)
        consumer_messages(stub, num_messages)
        order_messages(stub, num_messages)
        product_messages(stub, num_messages)
        stock_messages(stub, num_messages)

if __name__ == "__main__":
    logging.basicConfig()
    run()
