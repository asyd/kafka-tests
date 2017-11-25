import asyncio
from aiokafka import AIOKafkaProducer
from common import config_load
import datetime


async def producer():
    config = config_load()
    kafka_producer = AIOKafkaProducer(
        loop=loop,
        bootstrap_servers=config['kafka']['brokers']
    )
    await kafka_producer.start()
    try:
        count = 0
        while True:
            count += 1
            await kafka_producer.send_and_wait(config['kafka']['topic'],
                                               value=b"Hello world",
                                               # key=b",".join([count, datetime.datetime.now()]),
                                               # value=b"Hello world {}".__format__(datetime.datetime.now)
                                               )
            print("Send messaged: {}".format(count))
            await asyncio.sleep(config['kafka']['producer']['sleep'])
    finally:
        await kafka_producer.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(producer())
