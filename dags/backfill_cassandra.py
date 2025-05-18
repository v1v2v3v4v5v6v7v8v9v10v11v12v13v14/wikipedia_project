from pymongo import MongoClient, ReplaceOne
import json
import os
import logging
import pytest
from pathlib import Path
from pymongo.errors import PyMongoError
from icecream import ic
from logging.handlers import RotatingFileHandler
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Union
from tests.parser_test import SchemaTester
from persistence.mongo_service import MongoPersistenceService
from processing.parser.maria_parser import PageLinksParser
import sys
from icecream import ic
from tests.parser_test import ParserTestConfig
from processing.parser.base_parser import BaseParser
from processing.parser.xml_parser import RevisionParser
from processing.parser.tsv_parser import TSVParser
from processing.parser.maria_parser import PageLinksParser, PageInfoParser
from dataclasses import dataclass

@dataclass
class PageInfoRecord:
    id: int
    title: str
    timestamp: datetime

BASE_PATH = Path("/Applications/wikipedia_project")

test_config = {
            "PageInfo": (
                str(
                    BASE_PATH
                    / "output"
                    / "downloads"
                    / "/Users/danielkarwoski/Downloads/enwiki-20250501-page.sql"
                ),
                lambda code, ts: PageInfoParser(code, logger=logging.getLogger('PageInfo_mongo')),
                "pageinfo_topic",
            ),
        }

def load_test_data(test_name:str, test_config: Dict, sample_limit = 7):
    # Directly use the provided test_config mapping
    
    sample_path, parser_factory, topic = test_config[test_name]
    wiki_code, timestamp = ic(ParserTestConfig.extract_wiki_metadata(sample_path))
    # Instantiate parser
    parser = parser_factory(wiki_code, timestamp)
    # Ensure parser uses the correct wiki code
    parser.wiki_code = wiki_code
    parser.timestamp = timestamp
    # Generate records
    records = list(parser.parse_from_files([sample_path], sample_limit=sample_limit))
    return records

def task_process_test_configs():
    results = {}
    current_timestamp = datetime.now()
    for items in test_config.items():
        test_name, (sample_path, parser_factory, topic) = items
        wiki_code, timestamp = ParserTestConfig.extract_wiki_metadata(sample_path)
        # Instantiate parser
        parser = parser_factory(wiki_code, timestamp)
        # Ensure parser uses the correct wiki code
        parser.wiki_code = wiki_code
        parser.timestamp = timestamp
        # Generate records
        records = list(parser.parse_from_files([sample_path], sample_limit=100))
        # Add single timestamp field to each record
        for record in records:
            record['timestamp'] = current_timestamp
        results[test_name] = records
    return results

from airflow.decorators import dag, task
from datetime import datetime, timedelta

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
    schedule=None,  # Note: schedule_interval is deprecated in Airflow 3.0, use schedule instead
    catchup=False,
    tags=['backfill', 'cassandra'],
)
def process_test_configs_dag():
    
    @task
    def process_test_configs():
        results = {}
        current_timestamp = datetime.now()
        for items in test_config.items():
            test_name, (sample_path, parser_factory, topic) = items
            wiki_code, timestamp = ParserTestConfig.extract_wiki_metadata(sample_path)
            # Instantiate parser
            parser = parser_factory(wiki_code, timestamp)
            # Ensure parser uses the correct wiki code
            parser.wiki_code = wiki_code
            parser.timestamp = timestamp
            # Generate records
            records = list(parser.parse_from_files([sample_path], sample_limit=100))
            # Add single timestamp field to each record
            for record in records:
                record['timestamp'] = current_timestamp
            results[test_name] = records
        return results
    
    process_test_configs()

dag_instance = process_test_configs_dag()