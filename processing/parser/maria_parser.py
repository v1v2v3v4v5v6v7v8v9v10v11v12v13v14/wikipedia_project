"""Parser for MariaDB SQL dumps containing pagelinks data."""

import re
import time
import json
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from typing import Iterator, TextIO
from icecream import ic

from pyspark.sql import SparkSession
from confluent_kafka import Producer, KafkaException
from wiki_utils.datetime_utils import extract_date_from_filename
import os
from confluent_kafka import Producer  # ensure Producer is imported for default creation

class MariaDBParser:
    """Parser for MariaDB SQL dumps, processes INSERT INTO statements and sends to Kafka."""
    
    def __init__(self, spark: SparkSession, producer: Optional[Producer] = None, topic: str = 'pagelinks-topic', output_directory_path: str = 'output', timestamp: Optional[str] = None):
        """
        Initialize MariaDB parser.
        
        Args:
            spark: Initialized SparkSession
            producer: Configured Kafka producer
            topic: Kafka topic to publish to
            output_directory_path: Base directory for output
        """
        self.spark = spark
        # Provide defaults if not supplied
        if producer is None:
            producer = Producer({'bootstrap.servers': 'localhost:9092'})
        if topic is None:
            topic = 'pagelinks-topic'
        if not output_directory_path:
            output_directory_path = os.getcwd()
        self.producer = producer
        self.topic = topic
        self.output_directory_path = output_directory_path
        # Store timestamp for all downstream methods
        self.timestamp = timestamp

    def parse_from_files(
        self,
        file_streams: Iterator[TextIO],
        file_names: Optional[List[str]] = None,
        sample_limit: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Iterate over SQL dump streams, extract pagelink records.
        
        Args:
            file_streams: Iterable of opened file-like streams.
            file_names: Optional list of filenames (unused).
            sample_limit: Optional max number of records to yield.
        
        Yields:
            Dicts with keys: pl_from, pl_from_namespace, pl_target_id, timestamp.
        """
        count = 0
        for stream in file_streams:
            for raw_line in stream:
                # Decode bytes to string for processing
                try:
                    line = raw_line.decode('utf-8', errors='ignore')
                except AttributeError:
                    line = raw_line
                if not line.startswith("INSERT INTO"):
                    continue
                for pfrom, ns, target in MariaDBParser._extract_tuples(line):
                    record = {
                        "pl_from": int(pfrom),
                        "pl_from_namespace": int(ns),
                        "pl_target_id": int(target),
                        "timestamp": self.timestamp,
                    }
                    yield record
                    count += 1
                    if sample_limit and count >= sample_limit:
                        return
        
    def load_and_stream_data(self, file_path: str) -> None:
        """
        Load SQL dump, extract page link tuples, and stream to Kafka.
        
        Args:
            file_path: Path to SQL dump file
        """
        # Normalize and validate file path
        absolute_path = f"file://{str(file_path)}"
        
        # Load file using Spark
        rdd = self.spark.sparkContext.textFile(absolute_path)
        
        # Filter only INSERT INTO statements
        tuple_lines = rdd.filter(lambda line: line.startswith('INSERT INTO'))
        
        # Extract tuples using regex
        parsed_tuples = tuple_lines.flatMap(MariaDBParser._extract_tuples)
        
        # Convert strings to integers
        tuple_ints = parsed_tuples.map(lambda x: (int(x[0]), int(x[1]), int(x[2])))
        
        # Create DataFrame with column names
        df = tuple_ints.toDF(["pl_from", "pl_from_namespace", "pl_target_id"])
        
        # Show sample of data
        df.show()
        
        # Send rows to Kafka
        self._send_to_kafka(df)
        
    @staticmethod
    def _extract_tuples(line: str) -> List[tuple]:
        """
        Extract tuples from INSERT statement using regex.
        
        Args:
            line: SQL INSERT statement
            
        Returns:
            List of tuples (pl_from, pl_from_namespace, pl_target_id)
        """
        # allow optional extra flag columns after the third integer
        return re.findall(r'\((\d+),(\d+),(\d+)(?:,[^)]*)?\)', line)
        
    def _send_to_kafka(self, df) -> None:
        """
        Send DataFrame rows to Kafka.
        
        Args:
            df: DataFrame with page link data
        """
        for row in df.collect():
            record = {
                'pl_from': row['pl_from'],
                'pl_from_namespace': row['pl_from_namespace'],
                'pl_target_id': row['pl_target_id'],
                'timestamp': self.timestamp,
            }
            
            # Retry logic for buffer errors
            self._produce_with_retry(record)
            
        # Ensure all messages are sent (with a 5-second timeout)
        self.producer.flush(5)
        # Stop Spark to cleanly shut down the application
        self.spark.stop()
        # Exit the Python process to ensure CLI invocation terminates
        sys.exit(0)
        
    def _produce_with_retry(self, record: Dict[str, Any], max_retries: int = 5) -> None:
        """
        Send record to Kafka with retry logic.
        
        Args:
            record: Record to send
            max_retries: Maximum number of retries (default: 5)
        """
        retries = 0
        while retries < max_retries:
            try:
                self.producer.produce(
                    self.topic,
                    json.dumps(record).encode('utf-8'),
                    callback=self.delivery_report
                )
                # Poll to handle delivery callbacks
                self.producer.poll(0)
                break
            except BufferError:
                # Buffer full, wait and retry
                self.producer.poll(0)
                time.sleep(0.1)
                retries += 1
            except Exception as e:
                self.delivery_report(e, None)
                break
        
    def delivery_report(self, err, msg) -> None:
        """
        Kafka delivery callback.
        
        Args:
            err: Error or None
            msg: Produced message
        """
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def main() -> None:
    """Main function to run parser from command line."""
    spark = SparkSession.builder.appName("LoadSQLDump").getOrCreate()
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    topic = 'test'
    output_directory_path = os.environ.get("OUTPUT_DIRECTORY_PATH", "output")
    
    parser = MariaDBParser(spark, producer, topic, output_directory_path)
    
    # Find SQL files matching pattern
    files = list(Path(output_directory_path).glob("downloads/pagelinks/**/*.sql"))
    
    if files:
        parser.load_and_stream_data(files[0])
    else:
        raise FileNotFoundError("No SQL files matched your path pattern!")


if __name__ == "__main__":
    import os
    main()