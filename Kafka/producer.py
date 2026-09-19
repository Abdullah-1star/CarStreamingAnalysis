import pandas as pd 
from kafka import KafkaProducer
import json  
from pathlib import Path
import os
import time 

# some constants
KAFKA_TOPIC = 'kafka-flink-topic'
FILE_PATH = Path(__file__).parent.parent / 'Data' / '100k.csv'

# connect to kafka topic
producer = KafkaProducer (
    bootstrap_servers='localhost:9092',
    value_serializer= lambda value: json.dumps(value).encode('utf-8')

)

for chunk in pd.read_csv(FILE_PATH, chunksize=100_000):

    for _, row in chunk.iterrows():
        
        data = row.to_dict()
        producer.send(KAFKA_TOPIC, value=data)
        print(f"Sent: {data}")

        time.sleep(1)
    
producer.flush()        
print("All messages sent to Kafka topic:", KAFKA_TOPIC)