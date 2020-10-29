import asyncio
# from libs.ingest import MinioClient, RedisClient
from libs.ingest import process_json_message
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers


# mc = MinioClient()
# rc = RedisClient()

async def run(loop):
    nc = NATS()

    await nc.connect("192.168.64.102:4222", loop=loop)

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        process_json_message(data)
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))
        await nc.publish(subject=msg.reply, payload=b'Metric ingested')

    await nc.subscribe(subject='ingest_metric',
                       queue='workers',
                       cb=message_handler)


if __name__ == '__main__':
    print('Running...')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.run_forever()

# async def run(loop):
#     nc = NATS()
#
#     await nc.connect("192.168.64.102:4222", loop=loop)
#
#     async def message_handler(msg):
#         subject = msg.subject
#         reply = msg.reply
#         data = msg.data.decode()
#         mc.ingest(data)
#         print("Received a message on '{subject} {reply}': {data}".format(
#             subject=subject, reply=reply, data=data))
#         await nc.publish(subject=msg.reply, payload=b'Metric ingested')
#
#     await nc.subscribe(subject='ingest_metric',
#                        queue='minio_workers',
#                        cb=message_handler)
#
#
# if __name__ == '__main__':
#     print('Running...')
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(run(loop))
#     loop.run_forever()
