# Standard library imports
import os
from pathlib import Path

# Base directory of the project (``config`` lives one level below the root)
ROOT_DIR = Path(__file__).resolve().parents[1]

# Database configuration details for connecting to MongoDB
# Adjust 'host' to 'mongodb' when using Docker Compose networking
# This allows the application to connect to the MongoDB service defined in the Docker Compose file.
DATABASE_CONFIG = {
    'host': 'mongodb',  # Use 'mongodb' as the service name in Docker Compose
    'port': 27017,
    'db_name': 'wikipedia_db',
    'username': 'user',
    'password': 'password',
}

# Parser configuration settings, useful for adjusting parsing behavior in containerized environments
# Ensure that batch_size is set according to the resources allocated to the Docker container
PARSER_CONFIG = {
    'batch_size': 1000,  # Adjust based on container resources to optimize performance
    'log_level': 'INFO', # Docker-friendly logging level to capture necessary information without excessive verbosity
}

# Downloader configuration, paths should align with Docker volumes for persistent storage
# Make sure the download_dir matches the path of the mounted volume in the Docker container
DOWNLOAD_CONFIG = {
    # Path where downloaded dumps will be stored
    'download_dir': os.environ.get(
        'DOWNLOAD_DIR', str(ROOT_DIR / 'data' / 'downloads')
    ),
    'max_retries': int(os.environ.get('DOWNLOAD_MAX_RETRIES', 3)),
    'retry_delay': int(os.environ.get('DOWNLOAD_RETRY_DELAY', 2)),
}

# Path to the Avro schema file used by MongoPersistenceService
AVRO_SCHEMA_PATH = os.environ.get(
    'AVRO_SCHEMA_PATH', str(ROOT_DIR / 'avro_utils' / 'avro_schemas.json')
)

# Location of the persistence service log file
PERSISTENCE_LOGFILE_PATH = os.environ.get(
    'PERSISTENCE_LOGFILE_PATH', str(ROOT_DIR / 'logs' / 'persistence.log')
)

# Base directory for output and temporary files
OUTPUT_DIRECTORY_PATH = os.environ.get(
    'OUTPUT_DIRECTORY_PATH', str(ROOT_DIR / 'output')
)

# Kafka Configuration
KAFKA_BROKERS = 'localhost:9092'
KAFKA_TOPIC = 'test'

# Location of the Flink job used by Airflow's backfill pipeline
# Default points to the example union script in this repository
FLINK_JOB_PATH = os.environ.get(
    'FLINK_JOB_PATH', str(ROOT_DIR / 'processing' / 'flink_processing' / 'kafka_topic_union.py')
)

# Kafka partition key mapping
PARTITION_KEY_MAPPING = {
"PageView": ["page_hash", "year_month"],
"ClickStream": ["page_hash", "year_month"],
"OutboundPageLinks": ["year_month", "tuple_struct.pl_from"],
"InboundPageLinks": ["year_month", "tuple_struct.pl_target_id"],
"RevisionData": ["page_hash", "year_month"],
"WikiDocument": ["page_hash", "wiki_code", "year_month"],
"TitleIdMapping": ["page_hash", "year_month"]
}
