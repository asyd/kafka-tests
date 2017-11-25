import asyncio
from aiokafka import AIOKafkaConsumer
from common import config_load


async def consumer(instance):
    config = config_load()
    kafka_consumer = AIOKafkaConsumer(
        config['kafka']['topic'],
        loop=loop,
        bootstrap_servers=config['kafka']['brokers'],
        group_id=config['kafka']['consumer']['group_id'],
        auto_commit_interval_ms=1000,
    )
    await kafka_consumer.start()
    try:
        async for msg in kafka_consumer:
            print("instance: {}, topic: {}, partition: {}, key: {}, message: {}".format(
                instance,
                msg.topic,
                msg.partition,
                msg.key,
                msg.value
            ))
            await asyncio.sleep(config['kafka']['consumer']['sleep'])
    finally:
        await kafka_consumer.stop()


if __name__ == '__main__':
    config = config_load()
    loop = asyncio.get_event_loop()

    loop.run_until_complete(
        asyncio.gather(*[consumer(instance) for instance in range(config['kafka']['consumer']['instances'])]))
