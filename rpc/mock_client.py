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
        message = (mock.consumer_data())
        log_message.append(contract_pb2.ConsumerDataMessage(user_id=message["user_id"],
                                                   name=message["name"],
                                                   surname=message["surname"],
                                                   city=message["city"],
                                                   born_date=str(message["born_date"]),
                                                   register_date=str(message["register_date"])))

    response = stub.ConsumerData(iter(log_message))
    print("LogMessage client received: " + response.response)

def order_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.order_data())
        log_message.append(contract_pb2.OrderDataMessage(user_id=message["user_id"],
                                                   product_id=message["product_id"],
                                                   quantity=message["quantity"],
                                                   purchase_date=str(message["purchase_date"]),
                                                   payment_date=str(message["payment_date"]),
                                                   shipping_date=str(message["shipping_date"]),
                                                   delivery_date=str(message["delivery_date"])))

    response = stub.OrderData(iter(log_message))
    print("LogMessage client received: " + response.response)

def product_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.product_data())
        log_message.append(contract_pb2.ProductDataMessage(product_id=message["product_id"],
                                                   name=message["name"],
                                                   image=message["image"],
                                                   price=message["price"],
                                                   description=message["description"]))

    response = stub.ProductData(iter(log_message))
    print("LogMessage client received: " + response.response)

def stock_messages(stub, num_messages: int):
    log_message = []
    for _ in range(num_messages):
        message = (mock.stock_data())
        log_message.append(contract_pb2.StockDataMessage(product_id=message["product_id"],
                                                   quantity=message["quantity"]))

    response = stub.StockData(iter(log_message))
    print("LogMessage client received: " + response.response)

size_messages = 100
num_messages = 10_000
num_processes = 10

def run(_):
    with grpc.insecure_channel('localhost:50051') as channel:
        for _ in range(num_messages):
            stub = contract_pb2_grpc.DataServiceStub(channel)
            order_messages(stub, size_messages)
if __name__ == "__main__":
    logging.basicConfig()
    with multiprocessing.Pool(num_processes) as pool:
        pool.map(run, range(num_processes))
