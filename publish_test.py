from time import sleep
import asyncio, json, random, datetime
from libs.ingest import MinioClient
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

nc = NATS()


async def publish(loop):
    nc = NATS()
    await nc.connect("192.168.64.102:4222", loop=loop)
    x = 3000
    while x >= 0:
        message = json.dumps(randomize_json()).encode('utf-8')
        try:
            response = await nc.request(subject='ingest_metric',
                                        payload=message,
                                        timeout=1)
            print(response.data.decode())
            await nc.flush()
        except ErrTimeout:
            print('Request timed out')
        x = x - 1
        sleep(1)


def randomize_json():
    now = datetime.datetime.now()
    now.strftime("%H:%M:%S.%f - %b %d %Y")
    randomized_json = {
        "systemdata": {
            "timestamp": now.strftime("%H:%M:%S.%f - %b %d %Y"),
            "ingestNode": "ingester1.kube.beer"
        },
        "metadata": {
            "devicename": "kortlandpad-iot-livingroom"
        },
        "data": {
            "metrics": [
                {
                    "name": "temperature",
                    "type": "gauge",
                    "tags": [
                        "sensor-1",
                        "dunno"
                    ],
                    "value": random.randint(0, 43)
                },
                {
                    "name": "humidity",
                    "type": "gauge",
                    "tags": [
                        "sensor-3",
                        "good"
                    ],
                    "value": random.randint(0, 100)
                }
            ]
        }
    }
    return randomized_json
# --------------------------------------------------------------


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(publish(loop))
    loop.close()
