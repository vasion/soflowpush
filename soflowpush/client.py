import json
import os

import time

from soflowpush import messages
import pika


class Client(object):
    def __init__(self, queue, routing_key, user, password, exchange="", host="0.0.0.0", port=5672):
        self.port = port
        self.host = host
        self.exchange = exchange
        self.password = password
        self.user = user
        self.routing_key = routing_key
        self.queue = queue

    def send_single_message(self, message):
        assert isinstance(message, messages.Message)
        import pika
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port, credentials=pika.PlainCredentials(self.user, self.password))
            )
        channel = connection.channel()
        channel.queue_declare(queue=self.queue, durable=True)
        channel.basic_publish(exchange=self.exchange, routing_key=self.routing_key, body=json.dumps(message.__dict__))
        connection.close()

if __name__=="__main__":
    c = Client("queue.soflowstaging", "routing.soflowpush", "ufluser", os.environ.get("UFL_RABBIT_PASS", "pass"), "exchange.soflowpush", "rabbit.ultimatefanlive.com")
    now = time.time()
    end = now + 3
    t = messages.MultiPush(list([30,30, 30]), "hello", now, end)
    c.send_single_message(t)