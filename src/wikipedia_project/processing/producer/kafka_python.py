from processing.producer.kafka_producer import KafkaProducer
from config.settings import KAFKA_BROKERS, KAFKA_TOPIC
import json  # added for serialization

class ParserKafkaInterface:
    def __init__(self, brokers=None, topic=None):
        self.kafka_producer = KafkaProducer(brokers=brokers, topic=topic)

    def send_parsed_record(self, record, partition_key = None):
        serialized_record = json.dumps(record).encode('utf-8')  # serialization step added
        self.kafka_producer.send(serialized_record)

    def flush(self):
        self.kafka_producer.flush()

def parse_xml(file):
    # Example parsing logic (replace with your real parsing logic)
    yield {"id": 1, "content": "Example content"}
    yield {"id": 2, "content": "Another example"}

def run_parser(file_path):
    kafka_interface = ParserKafkaInterface(KAFKA_BROKERS, KAFKA_TOPIC)

    for record in parse_xml(file_path):
        kafka_interface.send_parsed_record(record)

    kafka_interface.flush()

if __name__ == "__main__":
    run_parser("example.xml")