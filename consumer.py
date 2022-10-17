import threading, time
from kafka import KafkaConsumer
from json import loads, dumps

BOOTSTRAP_SERVERS = "13.114.187.232:9092"
TOPICS = "test"
CONSUMER_GROUP = "test-group"

class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS,
                                 auto_offset_reset='earliest',
                                 group_id=CONSUMER_GROUP,
                                 value_deserializer=lambda x: loads(x.decode('utf-8')),
                                 max_poll_records = 2,
                                 consumer_timeout_ms=1000)
        
        consumer.subscribe([TOPICS])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()
