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
    'download_dir': '/Users/danielkarwoski/Downloads/wikipedia_refactor_target/data/downloads',
    'max_retries': 3,
    'retry_delay': 2,
}
AVRO_SCHEMA_PATH = '/Users/danielkarwoski/Downloads/wikipedia_refactor_target/avro_utils/avro_schemas.json'
PERSISTENCE_LOGFILE_PATH = '/Users/danielkarwoski/Downloads/wikipedia_refactor_target/persistence/mongodb_log.py'
OUTPUT_DIRECTORY_PATH = '/Users/danielkarwoski/Downloads/wikipedia_refactor_target/output/'
