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
    'download_dir': '/Users/danielkarwoski/Downloads/wikipedia_project/data/downloads',
    'max_retries': 3,
    'retry_delay': 2,
}
AVRO_SCHEMA_PATH = '/Applications/wikipedia_project/avro_utils/avro_schemas.json'
PERSISTENCE_LOGFILE_PATH = '/Users/danielkarwoski/Downloads/wikipedia_project/persistence/mongodb_log.py'
OUTPUT_DIRECTORY_PATH = '/Users/danielkarwoski/Downloads/wikipedia_project/output/'

# Kafka Configuration
KAFKA_BROKERS = 'localhost:9092'
KAFKA_TOPIC = 'test'

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
