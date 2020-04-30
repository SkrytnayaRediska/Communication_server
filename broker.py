#!/usr/bin/env python
import pika
from multiprocessing import Process


def callback(ch, method, properties, body):
    print(" [x] Received ", body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)


class BrokerProcess(Process):
    def __init__(self, queue):
        self.queue = queue
        Process.__init__(self)
    def run(self):

        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='mess')
        channel.basic_consume(queue='mess', on_message_callback=callback)
        print(' [*] Waiting for messages...')
        channel.start_consuming()

broker = BrokerProcess('mess')
broker.start()