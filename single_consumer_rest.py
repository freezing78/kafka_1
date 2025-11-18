import requests
import time
import logging
import json
import os
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RESTSingleConsumer:
    def __init__(self, rest_url, topic, group_id):
        self.rest_url = rest_url  # Основной URL REST Proxy (например: http://10.127.1.2:48084)
        self.topic = topic
        self.group_id = group_id
        self.session = requests.Session()
        self.consumer_instance = None
        # Генерируем уникальное имя для consumer
        self.consumer_name = f"single-consumer-{uuid.uuid4().hex[:8]}"
        self.create_consumer()

    def create_consumer(self):
        """Создание consumer instance"""
        payload = {
            "name": self.consumer_name,
            "format": "json",
            "auto.offset.reset": "earliest",
            "auto.commit.enable": "true"
        }

        try:
            response = self.session.post(
                f"{self.rest_url}/consumers/{self.group_id}",
                headers={"Content-Type": "application/vnd.kafka.v2+json"},
                data=json.dumps(payload)
            )

            if response.status_code == 200:
                self.consumer_instance = response.json()

                # ЗАМЕНА: Заменяем внутренний Docker URL на внешний
                base_uri = self.consumer_instance['base_uri']
                # Заменяем kafka-rest-proxy:8082 на наш внешний URL
                external_base_uri = base_uri.replace(
                    'http://kafka-rest-proxy:8082',
                    self.rest_url
                )
                self.consumer_instance['base_uri'] = external_base_uri

                logger.info(f"Consumer created: {self.consumer_instance['instance_id']}")
                logger.info(f"Using base_uri: {external_base_uri}")
            else:
                logger.error(f"Failed to create consumer: {response.text}")
                raise Exception(f"Consumer creation failed: {response.text}")

        except Exception as e:
            logger.error(f"Error creating consumer: {e}")
            raise

    def subscribe(self):
        """Подписка на топик"""
        payload = {
            "topics": [self.topic]
        }

        try:
            response = self.session.post(
                f"{self.consumer_instance['base_uri']}/subscription",
                headers={"Content-Type": "application/vnd.kafka.v2+json"},
                data=json.dumps(payload)
            )

            if response.status_code == 204:
                logger.info(f"Subscribed to topic: {self.topic}")
            else:
                logger.error(f"Subscription failed: {response.text}")
        except Exception as e:
            logger.error(f"Subscription error: {e}")
            raise

    def consume_single(self):
        """Потребление одного сообщения"""
        try:
            response = self.session.get(
                f"{self.consumer_instance['base_uri']}/records",
                headers={"Accept": "application/vnd.kafka.json.v2+json"},
                timeout=30
            )

            if response.status_code == 200:
                records = response.json()
                if records:
                    for record in records:
                        self.process_message(record)
                return len(records)
            else:
                logger.error(f"Consume error: {response.text}")
                return 0
        except Exception as e:
            logger.error(f"Consume error: {e}")
            return 0

    def process_message(self, record):
        """Обработка сообщения"""
        try:
            value = record.get('value', {})
            logger.info(f"Single REST Consumer processed: {value} "
                        f"[Partition: {record.get('partition')}, Offset: {record.get('offset')}]")

        except Exception as e:
            logger.error(f"Message processing error: {e}")

    def cleanup(self):
        """Удаление consumer instance при завершении"""
        if self.consumer_instance:
            try:
                response = self.session.delete(
                    f"{self.consumer_instance['base_uri']}",
                    headers={"Content-Type": "application/vnd.kafka.v2+json"}
                )
                if response.status_code == 204:
                    logger.info("Consumer instance deleted successfully")
                else:
                    logger.error(f"Failed to delete consumer: {response.text}")
            except Exception as e:
                logger.error(f"Error deleting consumer: {e}")

    def run(self):
        try:
            self.subscribe()
            while True:
                records_processed = self.consume_single()
                if records_processed == 0:
                    time.sleep(0.5)  # Небольшая пауза если нет сообщений
        except KeyboardInterrupt:
            logger.info("Single consumer stopped by user")
        except Exception as e:
            logger.error(f"Single consumer error: {e}")
        finally:
            self.cleanup()


if __name__ == "__main__":
    # Используем IP:port для доступа с хоста
    rest_url = os.getenv('KAFKA_REST_URL', 'http://10.127.1.2:48084')
    topic = os.getenv('TOPIC_NAME', 'messages-topic')

    consumer = RESTSingleConsumer(
        rest_url,
        topic,
        "single-rest-group"
    )
    consumer.run()