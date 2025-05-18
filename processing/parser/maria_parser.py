import datetime
from datetime import timezone
import json
import logging
import os
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, TextIO, Tuple, Callable, Type, List, Union, BinaryIO
import re
from fastavro import parse_schema, validate, schemaless_writer
from fastavro._validation import ValidationError

from processing.parser.base_parser import SQLDumpParser
from datetime import datetime, timezone

# --- Schema-driven parsing infrastructure ---
import re
from typing import Any, Callable, Dict, Iterator, List, Optional, Pattern, TextIO, BinaryIO
from processing.parser.base_parser import SQLDumpParser
from wiki_utils.datetime_utils import extract_date_from_filename, normalize_timestamp_format
from datetime import datetime, timezone
from wiki_utils.hashing_utils import WikimediaIdentifiers

FieldDef = Tuple[str, Callable[[str], Any]]

class TableSchema:
    def __init__(
        self,
        table_name: str,
        fields: List[FieldDef],
        tuple_pattern: Pattern[str],
        post_process: Optional[Callable[[Dict[str, Any]], Iterator[Dict[str, Any]]]] = None
    ):
        self.table_name = table_name
        self.fields = fields
        self.tuple_pattern = tuple_pattern
        self.post_process = post_process

class GenericSQLParser(SQLDumpParser):
    def __init__(
        self,
        schema: TableSchema,
        wiki_code: Optional[str] = None,
        timestamp: Optional[str] = None,
        file_name: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        super().__init__(logger)
        self.schema = schema
        # derive wiki_code and timestamp like before
        self.wiki_code = wiki_code or ""
        self.timestamp = normalize_timestamp_format(timestamp)
        self.file_name = file_name

    def target_table(self) -> str:
        return self.schema.table_name

    def _process_tuple(self, tuple_text: str, file_name: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        # derive timestamp if needed
        ts = self.timestamp
        if ts is None and file_name:
            date_str = extract_date_from_filename(file_name)
            if date_str:
                ts = normalize_timestamp_format(date_str)

        # convert timestamp to milliseconds for Avro long logicalType timestamp-millis
        if isinstance(ts, datetime):
            # ensure UTC before timestamp
            ts = int(ts.replace(tzinfo=timezone.utc).timestamp() * 1000)
        elif isinstance(ts, (int, float)):
            ts = int(ts * 1000)

        m = self.schema.tuple_pattern.match(tuple_text)
        if not m:
            return
        groups = m.groups()
        names, cast_fns = zip(*self.schema.fields)
        values = [fn(g) for fn, g in zip(cast_fns, groups)]
        record = dict(zip(names, values))
        # compute year_month, parsing strings or numeric ts
        year_month = None
        if (file_date := extract_date_from_filename(file_name)):
            try:
                # handle YYYYMMDD or YYYY-MM-DD formats
                if file_date.isdigit() and len(file_date) == 8:
                    dt_obj = datetime.strptime(file_date, "%Y%m%d")
                else:
                    dt_obj = datetime.strptime(file_date, "%Y-%m-%d")
                year_month = dt_obj.strftime("%Y-%m")
            except Exception:
                self.logger.error(f"Error parsing date from filename {file_name}: {file_date}", exc_info=True)
                self.stats["errors"] += 1
                return
        tuple_struct = {key: value for key, value in record.items()}
        tuple_struct["timestamp"] = ts
        record = {
        "year_month": year_month,
        "wiki_code": self.wiki_code,
        "tuple_struct": tuple_struct,
        "timestamp": ts
        } 
        self.stats["processed"] += 1

        if self.schema.post_process:
            for rec in self.schema.post_process(record):
                logging.debug(rec)
                yield rec
        else:
            logging.debug(record)
            yield record

# --- Define pagelinks schema and parser ---
def _pagelinks_post(base: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    out = base.copy()
    inb = base.copy()
    # swap source/target in the tuple_struct
    out_struct = out["tuple_struct"]
    in_struct = {
        "pl_from": out_struct["pl_target_id"],
        "pl_from_namespace": out_struct["pl_from_namespace"],
        "pl_target_id": out_struct["pl_from"],
        "timestamp": out["timestamp"]
    }
    yield out
    yield {"year_month": out["year_month"], "wiki_code": out["wiki_code"], "tuple_struct": in_struct, "timestamp": out["timestamp"]}

pagelinks_schema = TableSchema(
    table_name="pagelinks",
    fields=[
        ("pl_from", int),
        ("pl_from_namespace", int),
        ("pl_target_id", int)
    ],
    tuple_pattern=re.compile(r"\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*"),
    post_process=_pagelinks_post
)

class PageLinksParser(GenericSQLParser):
    def __init__(self, wiki_code: Optional[str] = None, timestamp: Optional[str] = None,
                 file_name: Optional[str] = None, logger: Optional[logging.Logger] = None):
        super().__init__(pagelinks_schema, wiki_code, timestamp, file_name, logger)

# --- Define page schema and parser ---
page_schema = TableSchema(
    table_name="page",
    fields=[
        ("page_id", int), ("page_namespace", int), ("page_title", lambda s: s),
        ("page_is_redirect", int), ("page_is_new", int), ("page_random", float),
        ("page_touched", lambda s: s), ("page_links_updated", lambda s: s or None),
        ("page_latest", int), ("page_len", int), ("page_content_model", lambda s: s or None),
        ("page_lang", lambda s: s or None)
    ],
    tuple_pattern=re.compile(
        r"""\s*
        (\d+)\s*,\s*
        (\d+)\s*,\s*'([^']*)'\s*,\s*
        (\d+)\s*,\s*
        (\d+)\s*,\s*
        ([0-9]*\.?[0-9]+)\s*,\s*
        '(\d{14})'\s*,\s*
        (?:'(\d{14})'|NULL)\s*,\s*
        (\d+)\s*,\s*
        (\d+)\s*,\s*
        (?:'([^']*)'|NULL)\s*,\s*
        (?:'([^']*)'|NULL)\s*
        """,
        re.IGNORECASE | re.DOTALL | re.VERBOSE
    )
)

class PageInfoParser(GenericSQLParser):
    def __init__(self, wiki_code: Optional[str] = None, timestamp: Optional[str] = None,
                 file_name: Optional[str] = None, logger: Optional[logging.Logger] = None):
        super().__init__(page_schema, wiki_code, timestamp, file_name, logger)

    def _process_tuple(self, tuple_text: str, file_name: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """
        Process page tuples and attach a page_hash using page title and wiki_code.
        """
        for record in super()._process_tuple(tuple_text, file_name):
            # Extract the page title from the tuple_struct
            title = record.get("tuple_struct", {}).get("page_title")
            wiki_code = record.get("wiki_code")
            if wiki_code and title:
                # Generate a URL-safe page identifier hash
                record["page_hash"] = WikimediaIdentifiers.create_page_identifier(wiki_code, title)
            yield record