import asyncio

import asynqp
import logging


from soflowpush import messages, handlers



RABBIT_ROUTING_KEY = None
RABBIT_HOST = None
RABBIT_PORT = None
RABBIT_USERNAME = None
RABBIT_PASS = None
RABBIT_EXCHANGE_NAME = None
RABBIT_QUEUE_NAME = None

loop = asyncio.get_event_loop()


def cb(msg):
    handlers.logger.info("msg_recv 1 {}".format(msg.message_id))
    try:
        _msg = messages.Message.parse(msg.json())
    except messages.ParseError:
        print("Bad Message")
        return

    if isinstance(_msg, messages.TestMessage):
        loop.create_task(handlers.test_message_handler(_msg))
    if isinstance(_msg, messages.MultiPush):
        loop.create_task(handlers.multi_message_handler(_msg))
    if isinstance(_msg, messages.SinglePush):
        handlers.logger.info("out_semaphore {}".format(outstanding._value))
        loop.create_task(handlers.single_message_handler(_msg))

    msg.ack()


@asyncio.coroutine
def consume(rabbit_routing_key="routing.soflowpush",
            rabbit_host='0.0.0.0',
            rabbit_port=5672,
            rabbit_username='ufl',
            rabbit_pass='uflpass',
            rabbit_exchange_name='exchange.soflowpush',
            rabbit_queue_name='queue.soflowpush'):
    global RABBIT_ROUTING_KEY
    RABBIT_ROUTING_KEY = rabbit_routing_key
    global RABBIT_HOST
    RABBIT_HOST = rabbit_host
    global RABBIT_PORT
    RABBIT_PORT = rabbit_port
    global RABBIT_USERNAME
    RABBIT_USERNAME = rabbit_username
    global RABBIT_PASS
    RABBIT_PASS = rabbit_pass
    global RABBIT_EXCHANGE_NAME
    RABBIT_EXCHANGE_NAME = rabbit_exchange_name
    global RABBIT_QUEUE_NAME
    RABBIT_QUEUE_NAME = rabbit_queue_name
    connection = None
    channel = None
    while True:
        if connection is None:
            try:
                channel, connection, queue, exchange, routing_key = yield from create_connection(

                )
                consumer = yield from queue.consume(cb)
            except Exception as e:
                print(e)
                print("failed to establish")
        else:
            if connection.is_closed():
                try:
                    channel.close()
                    yield from connection.close()
                except Exception as e:
                    print(e)
                    pass  # could be automatically closed, so this is expected
                connection = None
        yield from asyncio.sleep(30)


@asyncio.coroutine
def create_connection():
    connection = yield from asynqp.connect(RABBIT_HOST, RABBIT_PORT,
                                           username=RABBIT_USERNAME, password=RABBIT_PASS,
                                           loop=asyncio.get_event_loop())
    channel = yield from connection.open_channel()

    exchange = yield from channel.declare_exchange(RABBIT_EXCHANGE_NAME, 'topic')

    queue = yield from channel.declare_queue(RABBIT_QUEUE_NAME)
    yield from queue.bind(exchange, RABBIT_ROUTING_KEY)
    return channel, connection, queue, exchange, RABBIT_ROUTING_KEY
