import json
import os
import logging
from typing import Dict, Any, Optional, List, Union
from confluent_kafka import Producer
from wikipedia_project.config.settings import KAFKA_TOPIC, KAFKA_BROKERS, PARTITION_KEY_MAPPING
from datetime import datetime

class KafkaProducer:
    """Kafka producer with schema-aware message handling and detailed logging."""
    
    def __init__(self, 
                 brokers: str = KAFKA_BROKERS, 
                 topic: str = KAFKA_TOPIC, 
                 schema_path: str = "/Applications/wikipedia_refactor_target/avro_utils/avro_schemas.json"):
        """
        Initialize Kafka producer with schema support.
        
        Args:
            brokers: Comma-separated list of Kafka brokers
            topic: Default Kafka topic
            schema_path: Path to Avro schema definitions
        """
        self.conf = {
            'bootstrap.servers': brokers,
            'security.protocol': 'PLAINTEXT',
            'api.version.request': True,
        }
        self.topic = topic
        self.schema_path = schema_path
        self.producer = Producer(self.conf)
        self.schemas = self._load_schemas()
        self.success_count = 0  # Track successful deliveries
        
        logging.info(f"Initialized KafkaProducer for topic {topic}")
        logging.info(f"Loaded {len(self.schemas)} schemas" if self.schemas else "No schemas loaded")

    def _load_schemas(self) -> Dict[str, Any]:
        """Load and organize Avro schemas from JSON file."""
        if not os.path.exists(self.schema_path):
            logging.error(f"Schema file not found: {self.schema_path}")
            return {}

        try:
            with open(self.schema_path, "r") as f:
                schema_json = json.load(f)
            
            schemas = {}
            if isinstance(schema_json, list):
                schemas = {s['name']: s for s in schema_json if s.get('type') == 'record'}
            elif isinstance(schema_json, dict):
                schemas = {k: v for k, v in schema_json.items() 
                          if isinstance(v, dict) and v.get('type') == 'record'}
            
            logging.debug(f"Available schemas: {list(schemas.keys())}")
            return schemas
            
        except Exception as e:
            logging.error(f"Failed to load schemas: {e}")
            return {}

    def _get_schema_name(self, message: Dict[str, Any]) -> str:
        """
        Determine appropriate schema for message.
        
        Rules:
        1. Use explicit _schema_name if provided
        2. Infer from tuple_struct contents
        3. Fall back to topic-based guess
        """
        if "_schema_name" in message:
            return message["_schema_name"]
            
        if 'tuple_struct' in message:
            ts = message['tuple_struct']
            if 'rev_id' in ts: return 'RevisionData'
            if 'access_type' in ts: return 'PageView'
            if 'target_page' in ts: return 'ClickStream'
            if 'pl_from' in ts: 
                return 'OutboundPageLinks' if 'page_hash' not in message else 'InboundPageLinks'
            if 'title' in ts: 
                return 'WikiDocument' if 'wiki_code' in message else 'TitleIdMapping'
                
        return self.topic.replace('_topic', '').capitalize()

    def delivery_report(self, err, msg):
        """Handle delivery callbacks from Kafka with detailed logging."""
        if err:
            logging.error(f"Delivery failed: {err}")
        else:
            self.success_count += 1
            logging.info(
                f"Successfully delivered message to {msg.topic()} "
                f"[partition {msg.partition()}] @ offset {msg.offset()}"
            )
            logging.debug(f"Message details: {msg.value().decode('utf-8')[:200]}...")  # Log first 200 chars

    def _serialize_message(self, message: Any) -> bytes:
        """ Safely serialize messages, converting datetime objects to ISO format strings."""
        def clean_datetimes(obj):
            if isinstance(obj, dict):
                return {k: clean_datetimes(k) for k,v in obj.items()}
            elif isinstance(obj, list):
                return [clean_datetimes(i) for i in obj]
            elif isinstance(obj, datetime):
                return obj.isoformat()
            else:
                return obj
            safe_msg = clean_datetimes(message)
            return json.dumps(safe_msg).encode("utf-8")
        
            


    def send(self, message: Union[Dict[str, Any], List[Dict[str, Any]]], partition_key: Optional[bytes] = None):
        """Send one or more messages to Kafka with enhanced logging."""
        messages = [message] if not isinstance(message, list) else message
        
        for msg in messages:
            try:
                msg_copy = msg.copy()
                schema_name = self._get_schema_name(msg_copy)
                msg_copy.pop('_schema_name', None)
                
                logging.info(f"Preparing to send message with schema '{schema_name}'")
                logging.debug(f"Full message content: {msg_copy}")

                # If a key was provided explicitly, use it and skip mapping
                if partition_key is not None:
                    self.producer.produce(
                        topic=self.topic,
                        key=partition_key,
                        value=self._serialize_message(msg_copy),
                        callback=self.delivery_report
                    )
                    self.producer.poll(0)
                    continue
                
                # Determine and encode partition key if configured
                partition_fields = PARTITION_KEY_MAPPING.get(schema_name)
                if partition_fields:
                    def get_by_path(obj, path):
                        for part in path.split('.'):
                            obj = obj[part]
                        return obj
                    key_str = "|".join(str(get_by_path(msg_copy, f)) for f in partition_fields)
                    key_auto = key_str.encode("utf-8")
                    self.producer.produce(
                        topic=self.topic,
                        key=key_auto,
                        value=self._serialize_message(msg_copy),
                        callback=self.delivery_report
                    )
                else:
                    self.producer.produce(
                        topic=self.topic,
                        value=self._serialize_message(msg_copy),
                        callback=self.delivery_report
                    )
                self.producer.poll(0)
                
            except Exception as e:
                logging.error(f"Failed to send message: {e}\nMessage: {msg_copy}")
                raise

    def flush(self):
        """Ensure all messages are delivered with summary logging."""
        remaining = self.producer.flush()
        logging.info(
            f"Flush completed. Total successful deliveries: {self.success_count}. "
            f"Messages still in queue: {remaining}"
        )
        if remaining > 0:
            logging.warning(f"{remaining} messages were not delivered")


class MessageProducer:
    """Abstract message producer interface with logging."""
    
    def send(self, message: Union[Dict, List[Dict]]):
        raise NotImplementedError
        
    def flush(self):
        raise NotImplementedError


class KafkaMessageProducer(MessageProducer):
    """Concrete Kafka producer implementing MessageProducer interface with logging."""
    
    def __init__(self, kafka_producer: KafkaProducer):
        self.producer = kafka_producer
        logging.info("Initialized KafkaMessageProducer wrapper")

    def send(self, message):
        logging.debug("Forwarding message to Kafka producer")
        self.producer.send(message)

    def flush(self):
        logging.debug("Flushing Kafka producer")
        self.producer.flush()


class SchemaTester:
    """Validates messages against schemas and produces them with logging."""
    
    def __init__(self, producer: MessageProducer):
        self.producer = producer
        logging.info("Initialized SchemaTester with message producer")

    def test_schema(self, message):
        """Validate and send message with logging."""
        logging.info("Testing schema and sending valid messages")
        self.producer.send(message)

    def close(self):
        """Clean up resources with logging."""
        logging.info("Closing SchemaTester and flushing messages")
        self.producer.flush()