#!/usr/bin/env python

import argparse
import os
import sys

if sys.version_info < (3, 5):
    print("Python 3.5 or later required because we need asyncio.")
    exit(1)

import asyncio

parser = argparse.ArgumentParser()
parser.add_argument("--routing-key", help="Routing Key For RabbitMQ", action="store", default="routing.soflowpush")
parser.add_argument("--host", help="RabbitMQ host", action="store", default="0.0.0.0")
parser.add_argument("--port", help="RabbitMQ port", action="store", default=5672, type=int)
parser.add_argument("--username", help="RabbitMQ username", action="store", default="guest")
parser.add_argument("--password", help="RabbitMQ password", action="store", default="guest")
parser.add_argument("--exchange-name", help="RabbitMQ password", action="store", default="exchange.soflowpush")
parser.add_argument("--queue-name", help="RabbitMQ queue", action="store", default="queue.soflowpush")
parser.add_argument("--fcm-api-key", help="FCM Queue", action="store")
parser.add_argument("--dry-run", help="FCM Queue", action="store_true")
parser.add_argument("--cloudwatch_logs", help="FCM Queue", action="store_true")
args = parser.parse_args()

if args.fcm_api_key is None:
    print("Please provide FCM API key")
    exit(1)

os.environ["SOFLOW_FCM_API_KEY"] = args.fcm_api_key
if args.dry_run:
    os.environ["SOFLOW_DRY_RUN"] = "1"

from soflowpush import worker

print(args)
loop = asyncio.get_event_loop()
loop.run_until_complete(
    worker.consume(args.routing_key, args.host, args.port,
                   args.username, args.password, args.exchange_name,
                   args.queue_name)
)
