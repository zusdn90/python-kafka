from kafka import KafkaProducer
from json import dumps
import time

producer = KafkaProducer(acks=0,
                         compression_type='gzip',
                         bootstrap_servers=['13.114.187.232:9092'],
                         key_serializer=str.encode,
                         value_serializer=lambda x: dumps(x).encode('utf-8')
                         )

start = time.time()

for i in range(10000):
    producer.send('test', key= 'key_' + str(i), value='value_' + str(i))
    producer.flush()

print("elapsed :", time.time() - start)
