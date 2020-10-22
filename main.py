import asyncio
from libs.ingest import MinioClient
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers


mc = MinioClient()


async def run(loop):
    nc = NATS()

    await nc.connect("localhost:4222", loop=loop)

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        mc.ingest(data)
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    await nc.subscribe("foo", cb=message_handler)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.run_forever()
    # loop.run_until_complete(run(loop))
    # loop.close()
