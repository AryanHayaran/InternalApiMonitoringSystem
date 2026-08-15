import asyncio
import random
from typing import Optional, Dict, Any
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError, ProducerClosed
from app.utils.loggers import get_logger
from app.core.config import Config
from app.schemas.service import ProducerResultModal

logger = get_logger()

class KafkaProducerClient:
    """
    A unified, production-ready Kafka client for producing messages.
    """
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.broker_url = Config.KAFKA_BROKER_URL
        self.topic_name = Config.KAFKA_TOPIC_NAME
        self.max_retries = Config.KAFKA_MAX_RETRIES
        self.retry_delay_s = Config.KAFKA_RETRY_DELAY_S
        # Serialises reconnects. Without it, every concurrent health check that finds
        # a dead producer would launch its own full retry loop — a connect storm
        # against a broker that is already down.
        self._connect_lock = asyncio.Lock()

    def _get_producer_config(self) -> Dict[str, Any]:
        config = {
            'bootstrap_servers': self.broker_url,
            'request_timeout_ms': Config.KAFKA_REQUEST_TIMEOUT_MS,
            'enable_idempotence': Config.KAFKA_ENABLE_IDEMPOTENCE,
            'acks': Config.KAFKA_ACKS,
            'compression_type': Config.KAFKA_COMPRESSION_TYPE,
            'value_serializer': lambda v: v.encode('utf-8'),
            'key_serializer': lambda k: str(k).encode('utf-8') if k is not None else None
        }
        
        if Config.KAFKA_USERNAME and Config.KAFKA_PASSWORD:
            config.update({
                'security_protocol': Config.KAFKA_SECURITY_PROTOCOL,
                'sasl_mechanism': 'PLAIN',
                'sasl_plain_username': Config.KAFKA_USERNAME,
                'sasl_plain_password': Config.KAFKA_PASSWORD,
            })
        return config

    async def _discard_producer(self):
        """Stop and drop the current producer so a retry does not leak it."""
        if self.producer is not None:
            try:
                await self.producer.stop()
            except Exception:
                pass
            self.producer = None

    async def connect(self):
        logger.info(f"Attempting to connect to Kafka broker at {self.broker_url}...")

        for attempt in range(self.max_retries):
            try:
                config = self._get_producer_config()
                self.producer = AIOKafkaProducer(**config)
                await self.producer.start()
                logger.info("Kafka producer connected successfully.")
                return
            except Exception as e:
                # Previously a new AIOKafkaProducer was built each attempt without
                # stopping the last one, leaking background tasks on every retry.
                await self._discard_producer()
                if isinstance(e, (KafkaConnectionError, KafkaError)):
                    logger.warning(
                        f"Connection attempt {attempt + 1}/{self.max_retries} failed: {e}. Retrying...")
                else:
                    logger.error(f"Unexpected error during connection: {e}", exc_info=True)

                if attempt + 1 == self.max_retries:
                    logger.error("All Kafka connection attempts failed.")
                    raise

                # Exponential backoff with jitter instead of a fixed delay.
                delay = min(self.retry_delay_s * (2 ** attempt), 30) + random.uniform(0, 1)
                await asyncio.sleep(delay)


    async def ensure_connected(self) -> bool:
        """
        Reconnect if the producer is missing. Returns True when usable.

        This is what lets the app recover from a broker that was down at boot —
        previously send_result returned False forever after that.
        """
        if self.producer is not None:
            return True
        async with self._connect_lock:
            if self.producer is not None:   # another coroutine won the race
                return True
            try:
                await self.connect()
                return True
            except Exception as e:
                logger.error(f"Kafka producer reconnect failed: {e}")
                return False

    async def close(self):
        if self.producer:
            logger.info("Closing Kafka producer connection...")
            await self.producer.stop()
            logger.info("Kafka producer closed.")

    async def send_result(self, result: ProducerResultModal) -> bool:
        if not await self.ensure_connected():
            return False

        try:
            # We use model_dump_json() to safely serialize complex types like UUIDs and Datetimes
            await self.producer.send_and_wait(
                self.topic_name,
                value=result.model_dump_json(),
                key=result.id
            )
            logger.info(f"Successfully sent message for service {result.id} to topic {self.topic_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to send Kafka message for service {result.id}: {e}", exc_info=True)
            if isinstance(e, (KafkaConnectionError, ProducerClosed)):
                # Drop it so the next call reconnects; otherwise a producer whose
                # broker died later would never recover.
                await self._discard_producer()
            return False

    async def health_check(self) -> bool:
        return self.producer is not None

producer_client = KafkaProducerClient()