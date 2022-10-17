import threading, time
from kafka import KafkaProducer
from json import loads, dumps

BOOTSTRAP_SERVERS = "13.114.187.232:9092"
TOPICS = "test"    
    
class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(acks="all",
                                 compression_type='gzip',
                                 bootstrap_servers=BOOTSTRAP_SERVERS,
                                 value_serializer=lambda x: dumps(x).encode('utf-8'))

        while not self.stop_event.is_set():
            producer.send(TOPICS, key= 'key_test', value='value_test')
            time.sleep(1)

        producer.close()