from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
import logging
from processing.producer.kafka_python import ParserKafkaInterface
from config.settings import KAFKA_BROKERS, KAFKA_TOPIC
from tests.parser_test import ParserTestConfig
from pathlib import Path
from processing.parser.maria_parser import PageInfoParser
import subprocess  # for calling flink job

BASE_PATH = Path("/Users/danielkarwoski/Downloads")

test_config = {
    "PageInfo": (
        str(BASE_PATH / "enwiki-20250501-page.sql"),
        lambda code, ts: PageInfoParser(code, logger=logging.getLogger('PageInfo_mongo')),
        "pageinfo_topic",
    ),
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='process_test_configs_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['backfill', 'cassandra', 'flink'],
)
def process_test_configs_dag():

    @task
    def parse_and_send_records():
        current_timestamp = datetime.now(timezone.utc)
        kafka_interface = ParserKafkaInterface(KAFKA_BROKERS, KAFKA_TOPIC)
        all_sent_records = []

        for test_name, (sample_path, parser_factory, topic) in test_config.items():
            wiki_code, timestamp = ParserTestConfig.extract_wiki_metadata(sample_path)
            parser = parser_factory(wiki_code, timestamp)

            records = parser.parse_from_files([sample_path], sample_limit=100)

            for record in records:
                record['timestamp'] = current_timestamp
                try:
                    kafka_interface.send_parsed_record(record)
                    all_sent_records.append(record)
                except Exception as e:
                    logging.error(f"Kafka send error: {e} | Record: {record}")

        kafka_interface.flush()
        return all_sent_records

    @task
    def trigger_flink_job():
        flink_script_path = '/Applications/wikipedia_project/processing/flink_processing/kafka_topic_union.py'
        command = [
            'python', flink_script_path
        ]
        try:
            subprocess.run(command, check=True)
            logging.info("Flink job executed successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Flink job execution failed: {e}")
            raise

    # Define task dependencies clearly
    parsed_records = parse_and_send_records()
    flink_task = trigger_flink_job()
    
    # Airflow task dependency (parse/send records -> trigger flink)
    parsed_records >> flink_task

# Proper DAG instantiation
dag_instance = process_test_configs_dag()