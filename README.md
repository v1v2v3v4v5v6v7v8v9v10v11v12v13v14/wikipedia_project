# Wikipedia Project

This repository contains tools for downloading and processing Wikipedia dump data.

## Configuration

Several settings are controlled via environment variables. If not provided,
defaults relative to the project root are used.

| Variable | Purpose | Default |
|----------|---------|---------|
| `DOWNLOAD_DIR` | Directory for downloaded dumps | `data/downloads` |
| `DOWNLOAD_MAX_RETRIES` | Number of download retries | `3` |
| `DOWNLOAD_RETRY_DELAY` | Delay between retries in seconds | `2` |
| `AVRO_SCHEMA_PATH` | Location of `avro_schemas.json` | `avro_utils/avro_schemas.json` |
| `PERSISTENCE_LOGFILE_PATH` | Path to persistence service log file | `logs/persistence.log` |
| `OUTPUT_DIRECTORY_PATH` | Base directory for output files | `output` |

Set these variables in your environment or a `.env` file before running the
scripts if you need different locations.

## Flink Example: Combining Topics

The `processing/flink_processing/kafka_topic_union.py` script shows how to
consume multiple Kafka topics in a single Flink job. The streams are merged with
`union` so that a single check against Cassandra can be performed for each
unified event. Adjust the topic names, Kafka properties and Cassandra query to
match your environment.
