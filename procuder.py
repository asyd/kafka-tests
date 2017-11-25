import asyncio
from aiokafka import AIOKafkaProducer
from common import config_load
import datetime


async def producer(instance):
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
                                               key=",".join([str(count), str(datetime.datetime.now())]).encode('UTF-8'),
                                               )
            print("Send messaged: {}, from instance: {}".format(count, instance))
            await asyncio.sleep(config['kafka']['producer']['sleep'])
    finally:
        await kafka_producer.stop()


if __name__ == '__main__':
    config = config_load()
    loop = asyncio.get_event_loop()

    loop.run_until_complete(
        asyncio.gather(*[producer(instance) for instance in range(config['kafka']['producer']['instances'])]))