import csv
import sys
import logging
from kafka import KafkaProducer
logging.basicConfig(level=logging.INFO)

def loader(filename,kafka_topic):
    kafka_servers = 'localhost:9092'
    producer = KafkaProducer(bootstrap_servers=[kafka_servers])
    with open('{}'.format(filename), 'r') as file:
        reader = csv.reader(file, delimiter = ',')
        for messages in reader:
            producer.send(kafka_topic, value='{},{}'.format(messages[0],messages[1]).encode())
            producer.flush()
def main():
    filename = sys.argv[1]
    topic = sys.argv[2]
    loader(filename,topic)
if __name__ == "__main__":
    main()