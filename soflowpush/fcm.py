import datetime
import json
import os
import aiohttp
import time
import asyncio
import logging
import watchtower

logger = logging.getLogger("soflowpush")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ft = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
ch.setFormatter(ft)
logger.addHandler(ch)
if os.environ.get("WATCHTOWER", False):
    logger.addHandler(watchtower.CloudWatchLogHandler(log_group=os.environ.get("WATCHTOWER_GROUP_NAME")))
logger.propagate = False

logger.info("Starting 1")

CONTENT_TYPE = "application/json"
FCM_END_POINT = "https://fcm.googleapis.com/fcm/send"
FCM_API_KEY = os.getenv('SOFLOW_FCM_API_KEY', None)
if FCM_API_KEY is None:
    raise Exception("No FCM Key found. Set env variable SOFLOW_FCM_API_KEY")
FCM_LOW_PRIORITY = 'normal'
FCM_HIGH_PRIORITY = 'high'
DRY_RUN = os.getenv("SOFLOW_DRY_RUN", False) == '1'
if DRY_RUN:
    print("Doing dry run calls to FCM")

sem = asyncio.Semaphore(1000)
aiosession = aiohttp.ClientSession()

async def call_to_server(msg):
    async with sem:
        topic = "user{}".format(msg.user_id)
        message = msg.message
        async with aiosession.post(FCM_END_POINT, data=json.dumps(body(topic, message)), headers=request_headers()) as resp:
            logger.info("{} {} {} {} {} {}".format("push_sent", 1, topic, msg.message, resp.status, time.time() - msg.target_timestamp))


def request_headers():
    return {
        "Content-Type": CONTENT_TYPE,
        "Authorization": "key=" + FCM_API_KEY,
    }


def body(topic, message):
    fcm_payload = dict()
    fcm_payload['to'] = '/topics/%s' % topic
    fcm_payload['priority'] = FCM_HIGH_PRIORITY
    fcm_payload["dry_run"] = DRY_RUN
    fcm_payload['notification'] = {
        'body': message,
        'title': "",
        'icon': "ic_push"
    }
    fcm_payload["sound"] = "Default"
    return fcm_payload


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    from soflowpush.messages import SinglePush
    msg = SinglePush(30, "test now {}".format(datetime.datetime.now()), 0)

    loop.run_until_complete(asyncio.ensure_future(call_to_server(msg)))
    print("done")
