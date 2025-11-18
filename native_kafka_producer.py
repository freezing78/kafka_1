import requests
import time
import logging
import json
import os
import random
import hashlib
from message_schema import Message, MessageSerializer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SmartRESTProducer:
    def __init__(self, rest_url, topic, num_partitions=5):
        self.rest_url = rest_url
        self.topic = topic
        self.num_partitions = num_partitions
        self.session = requests.Session()
        self.session.timeout = 10  # Таймаут для запросов
        self.serializer = MessageSerializer()
        self.distribution_counter = 0
        self.partition_stats = {i: 0 for i in range(num_partitions)}
        self.message_count = 0
        self.offset_tracker = {i: 0 for i in range(num_partitions)}

    def calculate_partition(self, message_data, strategy="balanced"):
        """Вычисление партиции для сообщения"""

        if strategy == "round_robin":
            partition = self.distribution_counter % self.num_partitions
            self.distribution_counter += 1
            return partition, "🔄 Round-robin"

        elif strategy == "by_type":
            type_weights = {"info": 0, "warning": 1, "error": 2, "debug": 3, "critical": 4}
            partition = type_weights.get(message_data['message_type'], 0) % self.num_partitions
            return partition, f"📊 By type: {message_data['message_type']}"

        elif strategy == "by_priority":
            priority = message_data['priority']
            partition = (priority - 1) % self.num_partitions
            return partition, f"⚡ By priority: {priority}"

        elif strategy == "hash_based":
            message_id_hash = hashlib.md5(message_data['message_id'].encode()).hexdigest()
            partition = int(message_id_hash, 16) % self.num_partitions
            return partition, "🎲 Hash-based"

        elif strategy == "by_system":
            system = message_data['metadata'].get('module', 'unknown')
            system_hash = hashlib.md5(system.encode()).hexdigest()
            partition = int(system_hash, 16) % self.num_partitions
            return partition, f"🔧 By system: {system}"

        else:
            strategies = ["round_robin", "by_type", "by_priority", "hash_based", "by_system"]
            chosen_strategy = random.choice(strategies)
            return self.calculate_partition(message_data, chosen_strategy)

    def produce_message(self, message: Message):
        """Отправка сообщения с явным указанием партиции"""
        try:
            message_json = self.serializer.serialize(message)
            message_data = message.to_dict()

            # Вычисление партиции
            partition, distribution_info = self.calculate_partition(message_data, "balanced")

            # Безопасное получение ожидаемого offset
            expected_offset = self.offset_tracker.get(partition, 0)

            logger.info("📤 SENDING MESSAGE:")
            logger.info(f"   ID: {message_data['message_id']}")
            logger.info(f"   Content: {message_data['content']}")
            logger.info(f"   Type: {message_data['message_type']}")
            logger.info(f"   Priority: {message_data['priority']}")
            logger.info(f"   Distribution: {distribution_info}")
            logger.info(f"   Target Partition: {partition}")
            logger.info(f"   Expected Offset: ~{expected_offset}")
            logger.info("-" * 50)

            payload = {
                "records": [
                    {
                        "key": message_data['message_id'],
                        "value": json.loads(message_json),
                        "partition": partition
                    }
                ]
            }

            # Отправка с таймаутом
            response = self.session.post(
                f"{self.rest_url}/topics/{self.topic}",
                headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
                json=payload,
                timeout=10  # Таймаут 10 секунд
            )

            if response.status_code == 200:
                result = response.json()
                offsets = result.get('offsets', [])

                for offset_info in offsets:
                    actual_partition = offset_info.get('partition')
                    actual_offset = offset_info.get('offset')

                    # Безопасная проверка значений
                    if (actual_partition is not None and
                            actual_offset is not None and
                            actual_partition < self.num_partitions):

                        self.partition_stats[actual_partition] += 1
                        self.message_count += 1

                        # Обновляем трекер offsets
                        self.offset_tracker[actual_partition] = actual_offset + 1

                        logger.info(f"✅ Partition {actual_partition}, Offset {actual_offset}")

                        # Безопасная проверка последовательности offsets
                        if expected_offset is not None and actual_offset < expected_offset:
                            logger.warning(f"⚠️  Unexpected offset: got {actual_offset}, expected >= {expected_offset}")
                    else:
                        logger.error(
                            f"❌ Invalid partition or offset: partition={actual_partition}, offset={actual_offset}")

                # Статистика каждые 5 сообщений
                if self.message_count % 5 == 0:
                    self._print_distribution_stats()

            else:
                logger.error(f"❌ REST API error: {response.status_code} - {response.text}")

        except requests.exceptions.Timeout:
            logger.error("❌ Request timeout - Kafka REST Proxy not responding")
        except requests.exceptions.ConnectionError:
            logger.error("❌ Connection error - cannot reach Kafka REST Proxy")
        except Exception as e:
            logger.error(f"❌ REST producer error: {e}")

    def _print_distribution_stats(self):
        """Вывод статистики распределения по партициям"""
        total = sum(self.partition_stats.values())
        logger.info("📊 CURRENT PARTITION DISTRIBUTION:")

        for partition in sorted(self.partition_stats.keys()):
            count = self.partition_stats[partition]
            percentage = (count / total) * 100 if total > 0 else 0
            status = "🟢 ACTIVE" if count > 0 else "🔴 INACTIVE"
            next_offset = self.offset_tracker.get(partition, 0)
            logger.info(
                f"   Partition {partition}: {count} msgs ({percentage:.1f}%) - next offset: {next_offset} - {status}")

        active_partitions = len([p for p in self.partition_stats.values() if p > 0])
        if active_partitions == self.num_partitions:
            logger.info("🎉 SUCCESS: Messages distributed across all partitions!")
        elif active_partitions > 1:
            logger.info(f"⚠️  Partial: Using {active_partitions} out of {self.num_partitions} partitions")
        else:
            logger.info("❌ WARNING: All messages in single partition!")

        logger.info("=" * 60)


def create_diverse_messages():
    """Генератор разнообразных сообщений для тестирования распределения"""
    message_types = ["info", "warning", "error", "debug", "critical"]
    priorities = [1, 2, 3, 4, 5]

    modules = ["authentication", "database", "network", "storage", "api", "cache"]
    environments = ["development", "staging", "production"]

    sample_contents = [
        "Система запущена успешно в {env}",
        "Обнаружена проблема в модуле {module}",
        "Ошибка {module} в среде {env}",
        "Завершена обработка запроса {module}",
        "Мониторинг {module} активен в {env}",
        "Восстановление {module} завершено",
        "Обновление конфигурации {module} в {env}"
    ]

    message_count = 0

    while True:
        message_type = random.choice(message_types)
        priority = random.choice(priorities)
        module = random.choice(modules)
        env = random.choice(environments)

        content_template = random.choice(sample_contents)
        content = content_template.format(module=module, env=env) + f" (#{message_count})"

        metadata = {
            "source": "smart-producer",
            "sequence": message_count,
            "environment": env,
            "module": module,
            "distribution_group": f"group_{message_count % 5}"
        }

        if message_type == "error":
            metadata["error_code"] = random.randint(1000, 9999)
            metadata["retry_count"] = random.randint(0, 3)
        elif message_type == "critical":
            metadata["alert_level"] = "high"
            metadata["notify_team"] = True
        elif message_type == "warning":
            metadata["severity"] = random.choice(["low", "medium", "high"])

        message = Message(
            content=content,
            message_type=message_type,
            priority=priority,
            metadata=metadata
        )

        yield message
        message_count += 1


def run_smart_producer():
    rest_url = os.getenv('KAFKA_REST_URL', 'http://10.127.1.2:48084')
    topic = os.getenv('TOPIC_NAME', 'messages-topic')

    producer = SmartRESTProducer(rest_url, topic, num_partitions=5)
    message_generator = create_diverse_messages()

    logger.info("🚀 Starting Smart Kafka Producer with partition balancing")
    logger.info(f"📡 REST URL: {rest_url}")
    logger.info(f"📝 Topic: {topic}")
    logger.info(f"🔢 Partitions: {producer.num_partitions}")
    logger.info("🎯 Smart distribution: round_robin, by_type, by_priority, hash_based, by_system")
    logger.info("=" * 60)

    try:
        for i, message in enumerate(message_generator):
            start_time = time.time()
            producer.produce_message(message)
            execution_time = time.time() - start_time

            # Корректируем задержку с учетом времени выполнения
            sleep_time = max(0, 2 - execution_time)
            time.sleep(sleep_time)

            if i >= 29:
                logger.info("✅ Reached 30 messages, stopping...")
                break

    except KeyboardInterrupt:
        logger.info("🛑 Smart Producer stopped by user")
    except Exception as e:
        logger.error(f"💥 Smart Producer error: {e}")
    finally:
        producer._print_distribution_stats()


if __name__ == "__main__":
    run_smart_producer()