import functools
import json

import asyncio
import random

import time

import aiohttp

from soflowpush import messages, worker
from asynqp.message import Message as AsyncpMessage

from soflowpush.fcm import call_to_server
from soflowpush.fcm import logger

outstading = 0


class WrongMessageType(Exception):
    pass


def require_message_type(m_type):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(msg, *args, **kwargs):
            if not isinstance(msg, m_type):
                raise WrongMessageType()
            return f(msg, *args, **kwargs)
        return wrapper
    return decorator


@require_message_type(messages.TestMessage)
async def test_message_handler(msg):
    print("OK")


@require_message_type(messages.MultiPush)
async def multi_message_handler(msg):
    global outstading
    loop = asyncio.get_event_loop()
    task = loop.create_task(worker.create_connection())
    channel, connection, queue, exchange, routing_key = await task

    user_ids = msg.user_ids
    random.shuffle(msg.user_ids)
    msg.ack()

    time_to_spend = msg.target_timestamp_end - msg.target_timestamp_start
    time_per_push = float(time_to_spend)/len(user_ids)
    target = time.time()
    logger.info("multimsg {} {} {}".format(len(user_ids), outstading, msg.message))
    for i, id in enumerate(user_ids):
        target += time_per_push
        s_msg = messages.SinglePush(user_id=id, message=msg.message, target_timestamp=target)
        exchange.publish(AsyncpMessage(s_msg.__dict__), routing_key)
        outstading += 1
        if i % 100 == 0: #give away control every 100
            await asyncio.sleep(0)

@require_message_type(messages.SinglePush)
async def single_message_handler(msg):
    loop = asyncio.get_event_loop()
    delay = msg.target_timestamp - time.time()
    delay = delay if delay>0 else 0
    now = loop.time()
    loop.call_at(now+delay, scheduled_cb, msg)
    msg.ack()


def scheduled_cb(msg):
    global outstading
    logger.info("{} {}".format("outstanding", outstading))
    outstading -= 1
    loop = asyncio.get_event_loop()
    loop.create_task(call_to_server(msg))





