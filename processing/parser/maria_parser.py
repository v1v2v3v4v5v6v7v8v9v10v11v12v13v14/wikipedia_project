"""Parser for MariaDB SQL dumps containing pagelinks data."""

import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterator
from typing import TextIO
from pyspark.sql import SparkSession
from processing.parser.base_parser import BaseParser
from wiki_utils.datetime_utils import extract_date_from_filename
from icecream import ic
from datetime import datetime

def ic_sig(func):
    import inspect
    from icecream import ic
    ic(inspect.signature(func))

class MariaDBParser(BaseParser):
    """Parser for MariaDB SQL dumps, processes INSERT INTO statements."""

    def __init__(self, file_name: Optional[str], timestamp: Optional[str] = None, spark: Optional[SparkSession] = None):
        """
        Initialize MariaDB parser.

        Args:
            spark: Initialized SparkSession
            file_paths: List of file paths to SQL dumps
            timestamp: Optional timestamp string
        """
        self.file_name = file_name
        # Initialize Spark session if not provided
        if spark is None:
            spark = SparkSession.builder.appName("MariaDBParser").getOrCreate()
        self.spark = spark
        # Derive timestamp from first file if not explicitly provided
        if timestamp is None and self.file_name:
            timestamp = extract_date_from_filename(self.file_name)
        # Convert timestamp string (YYYYMMDD) to epoch milliseconds if necessary
        if isinstance(timestamp, str):
            try:
                timestamp = int(datetime.strptime(timestamp, "%Y%m%d").timestamp() * 1000)
            except ValueError:
                # leave timestamp as-is if it doesn't match YYYYMMDD
                pass
        self.timestamp = timestamp

    def parse_from_files(
        self,
        files: Iterator[str],
        file_path: Optional[str] = None,
        sample_limit: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Load SQL dump files using Spark, extract tuples, and yield records.

        Args:
            files: Iterator of file paths.
            file_path: Optional single file path for backward compatibility.
            sample_limit: Optional max number of records to yield.
        """
        for f in files:
            if file_path is not None:
                path_to_use = file_path
            else:
                if isinstance(f, str):
                    path_to_use = f
                else:
                    path_to_use = getattr(f, 'name', None)
                    if path_to_use is None:
                        continue

            absolute_path = f"file://{path_to_use}"

            rdd = self.spark.sparkContext.textFile(absolute_path)
            tuple_lines = rdd.filter(lambda line: line.startswith('INSERT INTO'))
            parsed_tuples = tuple_lines.flatMap(MariaDBParser._extract_tuples)

            if sample_limit:
                parsed_tuples = self.spark.sparkContext.parallelize(parsed_tuples.take(sample_limit))

            timestamp_to_use = self.timestamp
            if timestamp_to_use is None:
                timestamp_to_use = extract_date_from_filename(path_to_use)

            for pl_from, pl_from_namespace, pl_target_id in parsed_tuples.collect():
                yield {
                    "pl_from": int(pl_from),
                    "pl_from_namespace": int(pl_from_namespace),
                    "pl_target_id": int(pl_target_id),
                    "timestamp": timestamp_to_use
                }

    def load_and_parse_data(self, file_path: str):
        """
        Load SQL dump and extract page link tuples as a DataFrame.

        Args:
            file_path: Path to SQL dump file

        Returns:
            DataFrame with columns pl_from, pl_from_namespace, pl_target_id
        """
        # Normalize and validate file path
        absolute_path = f"file://{file_path}"

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

        return df

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


    def parse_stream(
            self,
            sample_limit: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Entry point for orchestrator: parse all SQL dump files provided at init into records.

        Args:
            sample_limit: Optional max number of records to yield per file.

        Yields:
            Dicts with keys: pl_from, pl_from_namespace, pl_target_id, timestamp.
        """
        for file_path in self.file_name:
            with open(file_path, 'rb') as f:
                ic_sig(self.parse_from_files([f], sample_limit=sample_limit))
                yield from self.parse_from_files([f], sample_limit=sample_limit)
