from producer import Producer
from consumer import Consumer

import threading, time
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

BOOTSTRAP_SERVERS = "13.114.187.232:9092"
TOPICS = "test"
CONSUMER_GROUP = "test-group"


if __name__ == '__main__':
    # Create Topic
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        topic = NewTopic(name=TOPICS,
                         num_partitions=1,
                         replication_factor=1)
        admin.create_topics([topic])
    except Exception as e:
        print(e)

    tasks = [Producer(), Consumer()]

    for t in tasks:
        t.start()

    time.sleep(10)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()
