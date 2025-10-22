import asyncio
from typing import Optional, Dict, Any
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError
from app.utils.loggers import get_logger
from app.core.config import Config
from app.schemas.service import ProducerResultModal

logger = get_logger()

class KafkaProducerClient:
    """
    A robust, production-ready Kafka client for producing messages.
    It handles connection retries, authentication, and provides a clean interface for sending data.
    """
    def __init__(self):
        """
        Initializes the client with production-ready configurations.
        """
        self.producer: Optional[AIOKafkaProducer] = None
        self.broker_url = Config.KAFKA_BROKER_URL
        self.topic_name = Config.KAFKA_TOPIC_NAME
        self.max_retries = Config.KAFKA_MAX_RETRIES
        self.retry_delay_s = Config.KAFKA_RETRY_DELAY_S
        self.is_connected = False

    def _get_producer_config(self) -> Dict[str, Any]:
        """
        Builds the producer configuration based on environment settings.
        Uses only aiokafka-compatible parameter names.
        """
        config = {
            'bootstrap_servers': self.broker_url,
            'request_timeout_ms': Config.KAFKA_REQUEST_TIMEOUT_MS,
            'enable_idempotence': Config.KAFKA_ENABLE_IDEMPOTENCE,
            'acks': Config.KAFKA_ACKS,
            'compression_type': Config.KAFKA_COMPRESSION_TYPE,
        }
        
        # Add authentication if configured
        if Config.KAFKA_USERNAME and Config.KAFKA_PASSWORD:
            config.update({
                'security_protocol': Config.KAFKA_SECURITY_PROTOCOL,
                'sasl_mechanism': 'PLAIN',
                'sasl_plain_username': Config.KAFKA_USERNAME,
                'sasl_plain_password': Config.KAFKA_PASSWORD,
            })
            logger.info("Kafka authentication configured")
        
        return config

    async def _ensure_topic_exists(self):
        """
        Ensure the Kafka topic exists, create it if it doesn't.
        Simplified version that handles errors gracefully.
        """
        try:
            from aiokafka.admin import AIOKafkaAdminClient, NewTopic
            
            admin_client = AIOKafkaAdminClient(
                bootstrap_servers=self.broker_url,
                client_id='monitoring_admin'
            )
            
            await admin_client.start()
            
            # Check if topic exists by trying to describe it
            try:
                metadata = await admin_client.describe_topics([self.topic_name])
                logger.info(f"Kafka topic '{self.topic_name}' already exists")
            except Exception:
                # Topic doesn't exist, create it
                logger.info(f"Creating Kafka topic: {self.topic_name}")
                topic = NewTopic(
                    name=self.topic_name,
                    num_partitions=1,  # Single partition for simplicity
                    replication_factor=1
                )
                await admin_client.create_topics([topic])
                logger.info(f"Kafka topic '{self.topic_name}' created successfully")
                
            await admin_client.close()
            
        except Exception as e:
            logger.warning(f"Could not ensure topic exists: {e}. Topic may need to be created manually.")
            # Don't fail the connection if topic creation fails

    async def connect(self):
        """
        Initializes and starts the Kafka producer with a robust retry mechanism.
        This is essential for production startups where services may start in any order.
        """
        logger.info(f"Attempting to connect to Kafka broker at {self.broker_url}...")
        logger.info(f"Kafka configuration: topic={self.topic_name}, max_retries={self.max_retries}")
        
        # Ensure topic exists before connecting producer
        await self._ensure_topic_exists()
        
        for attempt in range(self.max_retries):
            try:
                config = self._get_producer_config()
                logger.debug(f"Kafka producer config: {config}")
                
                self.producer = AIOKafkaProducer(**config)
                await self.producer.start()
                self.is_connected = True
                logger.info("Kafka producer connected successfully.")
                return  # Exit the loop on success
            except (KafkaConnectionError, KafkaError) as e:
                logger.warning(
                    f"Kafka connection attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                    f"Retrying in {self.retry_delay_s} seconds..."
                )
                if attempt + 1 == self.max_retries:
                    logger.error(f"All Kafka connection attempts failed. Broker URL: {self.broker_url}")
                    logger.error("Application startup will fail.")
                    # Re-raise the final exception to halt application startup
                    raise
                await asyncio.sleep(self.retry_delay_s)
            except Exception as e:
                logger.error(f"Unexpected error during Kafka connection attempt {attempt + 1}: {e}", exc_info=True)
                if attempt + 1 == self.max_retries:
                    raise
                await asyncio.sleep(self.retry_delay_s)

    async def close(self):
        """Stops the Kafka producer connection gracefully."""
        if self.producer and self.is_connected:
            logger.info("Closing Kafka producer connection...")
            try:
                await self.producer.stop()
                self.is_connected = False
                logger.info("Kafka producer closed successfully.")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}", exc_info=True)

    async def send_result(self, result: ProducerResultModal) -> bool:
        """
        Serializes a Pydantic model and sends it to the Kafka topic.

        Args:
            result (ProducerResultModal): The data to send.
            
        Returns:
            bool: True if message was sent successfully, False otherwise.
        """
        if not self.producer or not self.is_connected:
            logger.error("Producer not connected. Cannot send message.")
            return False

        try:
            # Serialize the Pydantic model to a JSON string, then encode to bytes
            message_json = result.model_dump_json()
            message_bytes = message_json.encode('utf-8')
            
            # Send with delivery report callback for better error handling
            future = await self.producer.send_and_wait(
                self.topic_name, 
                message_bytes,
                key=str(result.id).encode('utf-8')  # Use service ID as key for partitioning
            )
            
            logger.info(f"Successfully sent message for service {result.id} to topic {self.topic_name}")
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error sending message for service {result.id}: {e}", exc_info=True)
            return False
        except Exception as e:
            # This will catch serialization errors or other unexpected failures
            logger.error(f"Failed to send Kafka message for service {result.id}: {e}", exc_info=True)
            return False

    async def health_check(self) -> bool:
        """
        Check if the producer is healthy and connected.
        
        Returns:
            bool: True if producer is healthy, False otherwise.
        """
        if not self.producer or not self.is_connected:
            return False
        
        try:
            # Try to get metadata to verify connection
            metadata = await self.producer.client.cluster
            return metadata is not None
        except Exception as e:
            logger.warning(f"Kafka producer health check failed: {e}")
            return False


producer_client = KafkaProducerClient()