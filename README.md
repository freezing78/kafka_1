# kafka_1
study

Apache Kafka Stack with Docker Compose
Локальный стэк для разработки и тестирования Apache Kafka с Zookeeper и Web UI.

Развертывание кластера^

Способ 1: Через Portainer (Web-интерфейс для управления Docker Compose)
Развертывание через Portainer на машине с IP 10.127.1.2

Способ 2: Через командную строку
mkdir kafka-cluster  
cd kafka-cluster  
Скопируйте предоставленное содержимое в файл docker-compose.yml в текущей директории  
docker-compose up -d  
docker-compose ps  

# Доступ к сервисам
Kafka UI: http://localhost:48090  
Schema Registry: http://localhost:48081  
Kafka REST Proxy: http://localhost:48084  
Kafka Brokers: localhost:49092, localhost:49093, localhost:49094  

# Компоненты стэка и параметры конфигурации
Zookeeper (v7.5.0) - порт 42181
ZOOKEEPER_CLIENT_PORT - порт для подключения клиентов (Kafka брокеров)
ZOOKEEPER_TICK_TIME - базовый временной интервал в миллисекундах для heartbeat и таймаутов

Kafka Broker (v7.5.0) - порты 49092, 49093, 49094
KAFKA_BROKER_ID - уникальный идентификатор брокера в кластере
KAFKA_ZOOKEEPER_CONNECT - адрес Zookeeper для координации кластера
KAFKA_LISTENERS - интерфейсы и порты, которые слушает брокер (0.0.0.0 = все интерфейсы)
KAFKA_ADVERTISED_LISTENERS - адреса, которые брокер сообщает клиентам для подключения
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR - фактор репликации для топика __consumer_offsets
KAFKA_DEFAULT_REPLICATION_FACTOR - фактор репликации по умолчанию для новых топиков

Kafka UI (latest) - порт 48090
KAFKA_CLUSTERS_0_NAME - имя кластера в UI
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS - подключение к Kafka брокерам
KAFKA_CLUSTERS_0_ZOOKEEPER - подключение к Zookeeper (для мониторинга)
KAFKA_CLUSTERS_0_SCHEMAREGISTRY - подключение к Schema Registry

Schema Registry (7.5.0) - порт 48081
SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS - Kafka брокеры для хранения схем
SCHEMA_REGISTRY_HOST_NAME - имя хоста сервиса
SCHEMA_REGISTRY_LISTENERS - HTTP endpoint для API Schema Registry

Kafka REST (7.5.0) - порт 48084
KAFKA_REST_BOOTSTRAP_SERVERS - подключение к Kafka кластеру
KAFKA_REST_LISTENERS - HTTP endpoint для REST API
KAFKA_REST_SCHEMA_REGISTRY_URL - адрес Schema Registry для работы со схемами

# Проверка работы кластера
# Проверить статус контейнеров
# Посмотреть логи и убедиться, что все контейнеры запущены без ошибок
docker-compose ps
# или
docker ps
Проверить через Kafka UI
Зайти в Kafka UI через web и создать тестовый топик
URL: http://localhost:48090

# Работа с топиками через консоль
# Подключитесь к контейнеру Kafka
docker exec -it kafka-cluster-kafka111-1 /bin/bash
# Выводит возможные команды
ls /usr/bin | grep kafka
# Создайте топик с 3 партициями и 2 репликами
kafka-topics --create \
  --topic messages-topic \
  --bootstrap-server kafka111:9092 \
  --partitions 3 \
  --replication-factor 2
# Проверьте созданный топик
kafka-topics --describe \
  --topic messages-topic \
  --bootstrap-server kafka111:9092

# Разработка приложений
Шаг 1: Создание топика
Создайте топик с 3 партициями и 2 репликами через консоль (команды выше).

Шаг 2: Создание приложения
Приложение состоит из:
1 продюсера - отправляет сообщения в Kafka-топик (модель push)
2 консьюмеров:
SingleMessageConsumer - считывает по одному сообщению, обрабатывает и коммитит оффсет автоматически
BatchMessageConsumer - считывает минимум по 10 сообщений за один poll, обрабатывает в цикле и коммитит оффсет после обработки пачки

Шаг 3: Сериализация и десериализация
Реализовать сериализацию и десериализацию
Формат данных выбрать по желанию

Шаг 4: Гарантии доставки сообщений
Обеспечить гарантии доставки сообщений
Запуск приложений

Для выполнения шагов 2, 3 и 4 запускайте файлы .py в PyCharm:
native_kafka_producer.py
batch_consumer_rest.py
single_consumer_rest.py
message_schema.py
requirements.txt
