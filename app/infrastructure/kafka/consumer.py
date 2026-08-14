import json
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from app.core.config import Config
from app.utils.loggers import get_logger

logger = get_logger()

class KafkaConsumerClient:
    """Core Kafka consumer — reads and yields messages asynchronously."""

    def __init__(self):
        self.consumer = None
        self.topic_name = Config.KAFKA_TOPIC_NAME
        self.broker_url = Config.KAFKA_BROKER_URL
        self.group_id = "monitoring_consumer_group"
        self.max_retries = Config.KAFKA_MAX_RETRIES or 5
        self.retry_delay_s = Config.KAFKA_RETRY_DELAY_S or 2

    async def connect(self):
        """Connect to Kafka broker with retries."""
        logger.info(f"Connecting Kafka consumer to broker: {self.broker_url}")
        self.consumer = AIOKafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.broker_url,
            group_id=self.group_id,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        
        for attempt in range(self.max_retries):
            try:
                await self.consumer.start()
                logger.info(f"Kafka consumer connected and listening to topic: {self.topic_name}")
                return
            except Exception as e:
                logger.warning(f"Consumer connection attempt {attempt + 1}/{self.max_retries} failed: {e}. Retrying in {self.retry_delay_s}s...")
                if attempt + 1 == self.max_retries:
                    logger.error("All Kafka consumer connection attempts failed.")
                    raise
                await asyncio.sleep(self.retry_delay_s)

    async def consume_messages(self):
        """Async generator that yields Kafka messages one by one."""
        if not self.consumer:
            await self.connect()

        try:
            async for message in self.consumer:
                logger.info(f"📩 Received message from topic {self.topic_name}: {message.value}")
                yield message.value
        except KafkaError as e:
            logger.error(f"Kafka error while consuming: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected consumer error: {e}", exc_info=True)
        finally:
            await self.close()

    async def close(self):
        """Gracefully close Kafka connection."""
        if self.consumer:
            logger.info("Closing Kafka consumer...")
            await self.consumer.stop()
            logger.info("Kafka consumer closed successfully.")