import asyncio
from aiokafka import AIOKafkaProducer
from common import config_load


async def consumer():
    config = config_load()
    kafka_producer = AIOKafkaProducer(
        loop=loop,
        bootstrap_servers=config['kafka']['brokers']
    )
    await kafka_producer.start()
    try:
        while True:
            await kafka_producer.send_and_wait(config['kafka']['topic'],
                                               b"Hello world")
            await asyncio.sleep(config['kafka']['producer']['sleep'])
    finally:
        await kafka_producer.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consumer())
