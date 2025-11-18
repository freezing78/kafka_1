import requests
import time
import logging
import json
import os
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RESTBatchConsumer:
    def __init__(self, rest_url, topic, group_id, batch_size=10):
        self.rest_url = rest_url
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.session = requests.Session()
        self.consumer_instance = None
        # Генерируем уникальное имя для consumer
        self.consumer_name = f"batch-consumer-{uuid.uuid4().hex[:8]}"
        self.create_consumer()

    def create_consumer(self):
        """Создание consumer instance с настройками для batch processing"""
        payload = {
            "name": self.consumer_name,
            "format": "json",
            "auto.offset.reset": "earliest",
            "auto.commit.enable": "false"  # Ручной коммит
            # REST Proxy не поддерживает fetch.min.bytes и fetch.max.wait.ms напрямую
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
                external_base_uri = base_uri.replace(
                    'http://kafka-rest-proxy:8082',
                    self.rest_url
                )
                self.consumer_instance['base_uri'] = external_base_uri

                logger.info(f"Batch consumer created: {self.consumer_instance['instance_id']}")
            else:
                logger.error(f"Failed to create batch consumer: {response.text}")
                raise Exception(f"Batch consumer creation failed: {response.text}")

        except Exception as e:
            logger.error(f"Error creating batch consumer: {e}")
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
                logger.info(f"Batch consumer subscribed to topic: {self.topic}")
            else:
                logger.error(f"Batch subscription failed: {response.text}")
        except Exception as e:
            logger.error(f"Subscription error: {e}")
            raise

    def consume_batch(self):
        """Потребление пачки сообщений"""
        batch = []
        start_time = time.time()

        # Накопление пачки - эмулируем batch поведение
        while len(batch) < self.batch_size and (time.time() - start_time) < 10:
            try:
                response = self.session.get(
                    f"{self.consumer_instance['base_uri']}/records",
                    headers={"Accept": "application/vnd.kafka.json.v2+json"},
                    timeout=10
                )

                if response.status_code == 200:
                    records = response.json()
                    if records:
                        batch.extend(records)
                        logger.info(f"Received {len(records)} messages, total: {len(batch)}")
                    else:
                        # Нет сообщений, ждем немного
                        time.sleep(0.5)
                else:
                    logger.error(f"Batch consume error: {response.text}")
                    break

            except Exception as e:
                logger.error(f"Batch consume request error: {e}")
                break

        # Обработка накопленной пачки
        if batch:
            self.process_batch(batch)
            self.commit_offsets()
            return len(batch)
        else:
            logger.info("No messages received in this batch")
            return 0

    def process_batch(self, batch):
        """Обработка пачки сообщений"""
        try:
            batch_start_time = time.time()

            for record in batch:
                value = record.get('value', {})
                logger.info(f"Batch REST Consumer processed: {value} "
                            f"[Partition: {record.get('partition')}, Offset: {record.get('offset')}]")

                # Имитация обработки
                time.sleep(0.1)

            processing_time = time.time() - batch_start_time
            logger.info(f"Batch processed: {len(batch)} messages in {processing_time:.2f}s")

        except Exception as e:
            logger.error(f"Batch processing error: {e}")

    def commit_offsets(self):
        """Ручной коммит оффсетов"""
        try:
            response = self.session.post(
                f"{self.consumer_instance['base_uri']}/offsets",
                headers={"Content-Type": "application/vnd.kafka.v2+json"}
            )

            if response.status_code in [200, 204]:
                logger.info("Offsets committed successfully")
            else:
                logger.error(f"Commit error: {response.text}")

        except Exception as e:
            logger.error(f"Commit error: {e}")

    def cleanup(self):
        """Удаление consumer instance при завершении"""
        if self.consumer_instance:
            try:
                response = self.session.delete(
                    f"{self.consumer_instance['base_uri']}",
                    headers={"Content-Type": "application/vnd.kafka.v2+json"}
                )
                if response.status_code == 204:
                    logger.info("Batch consumer instance deleted successfully")
                else:
                    logger.error(f"Failed to delete batch consumer: {response.text}")
            except Exception as e:
                logger.error(f"Error deleting batch consumer: {e}")

    def run(self):
        try:
            self.subscribe()
            batch_count = 0

            while True:
                batch_count += 1
                logger.info(f"Starting batch #{batch_count}")

                messages_processed = self.consume_batch()

                if messages_processed == 0:
                    logger.info("No messages, waiting before next batch...")
                    time.sleep(2)

        except KeyboardInterrupt:
            logger.info("Batch consumer stopped by user")
        except Exception as e:
            logger.error(f"Batch consumer error: {e}")
        finally:
            self.cleanup()


if __name__ == "__main__":
    # Используем IP:port для доступа с хоста
    rest_url = os.getenv('KAFKA_REST_URL', 'http://10.127.1.2:48084')
    topic = os.getenv('TOPIC_NAME', 'messages-topic')

    consumer = RESTBatchConsumer(
        rest_url,
        topic,
        "batch-rest-group",
        batch_size=10
    )
    consumer.run()