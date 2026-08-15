import json
import asyncio
from typing import Any, Dict, Optional
from aiokafka import AIOKafkaConsumer
from app.core.config import Config
from app.utils.loggers import get_logger

logger = get_logger()

class KafkaConsumerClient:
    """Core Kafka consumer — reads and yields messages asynchronously."""

    def __init__(self, group_id: str = None):
        self.consumer = None
        self.topic_name = Config.KAFKA_TOPIC_NAME
        self.broker_url = Config.KAFKA_BROKER_URL
        self.group_id = group_id or Config.KAFKA_CONSUMER_GROUP
        self.max_retries = Config.KAFKA_MAX_RETRIES or 5
        self.retry_delay_s = Config.KAFKA_RETRY_DELAY_S or 2

    def _get_consumer_config(self) -> Dict[str, Any]:
        config = {
            'bootstrap_servers': self.broker_url,
            'group_id': self.group_id,
            # Manual commit → at-least-once. Safe because incident detection recomputes
            # from the last-3 DB window, so reprocessing a message is idempotent.
            'enable_auto_commit': False,
            'auto_offset_reset': Config.KAFKA_AUTO_OFFSET_RESET,
            'max_poll_records': 100,
            'session_timeout_ms': 30000,
            'heartbeat_interval_ms': 3000,
            # NOTE: no value_deserializer on purpose. Decoding here would raise inside
            # aiokafka's fetcher, where it cannot be caught and skipped — the same record
            # would be re-fetched forever. See deserialize() below.
        }

        if Config.KAFKA_USERNAME and Config.KAFKA_PASSWORD:
            config.update({
                'security_protocol': Config.KAFKA_SECURITY_PROTOCOL,
                'sasl_mechanism': 'PLAIN',
                'sasl_plain_username': Config.KAFKA_USERNAME,
                'sasl_plain_password': Config.KAFKA_PASSWORD,
            })
        return config

    async def connect(self):
        """Connect to Kafka broker with retries."""
        logger.info(f"Connecting Kafka consumer (group={self.group_id}) to broker: {self.broker_url}")
        self.consumer = AIOKafkaConsumer(self.topic_name, **self._get_consumer_config())

        for attempt in range(self.max_retries):
            try:
                await self.consumer.start()
                logger.info(f"Kafka consumer connected and listening to topic: {self.topic_name}")
                return
            except Exception as e:
                logger.warning(
                    f"Consumer connection attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                    f"Retrying in {self.retry_delay_s}s..."
                )
                if attempt + 1 == self.max_retries:
                    logger.error("All Kafka consumer connection attempts failed.")
                    raise
                await asyncio.sleep(self.retry_delay_s)

    @staticmethod
    def deserialize(record) -> Optional[Dict[str, Any]]:
        """
        Decode a ConsumerRecord's value in OUR code, so a malformed payload is an
        ordinary exception the caller can log, commit past, and continue from.
        Returns None when the payload cannot be decoded.
        """
        try:
            return json.loads(record.value.decode("utf-8"))
        except Exception as e:
            logger.error(
                f"Undecodable message at {record.topic}[{record.partition}]@{record.offset}: {e}. "
                f"Raw bytes: {record.value!r:.200}"
            )
            return None

    async def consume_messages(self):
        """
        Async generator yielding raw ConsumerRecords.

        Deliberately does NOT catch exceptions or close the consumer — lifecycle and
        recovery belong to the supervising caller. Swallowing errors here previously
        ended the generator and let the process exit 0 on failure.
        """
        if not self.consumer:
            await self.connect()

        async for record in self.consumer:
            logger.info(
                f"📩 {self.topic_name}[{record.partition}]@{record.offset} "
                f"(group={self.group_id})"
            )
            yield record

    async def commit(self):
        """Commit current offsets. Called only after successful processing."""
        if self.consumer:
            try:
                await self.consumer.commit()
            except Exception as e:
                logger.error(f"Offset commit failed: {e}", exc_info=True)

    async def close(self):
        """Gracefully close Kafka connection."""
        if self.consumer:
            logger.info("Closing Kafka consumer...")
            try:
                await self.consumer.stop()
                logger.info("Kafka consumer closed successfully.")
            except Exception as e:
                logger.warning(f"Error while closing Kafka consumer: {e}")
            finally:
                self.consumer = None
