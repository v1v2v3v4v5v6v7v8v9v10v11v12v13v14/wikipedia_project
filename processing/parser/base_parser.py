# processing/parser/base_parser.py
from abc import ABC, abstractmethod
from processing.shared.file_utils import get_text_stream
from typing import Iterator, TextIO, Dict, Any, Optional, Union, List, Set, BinaryIO
import logging

from processing.shared.file_utils import get_binary_stream, get_text_stream, safe_close, is_binary_stream
import bz2
import gzip
import io
import re

class BaseParser(ABC):
    """Abstract base class for data parsers implementing parse_stream."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize parser with optional logger."""
        self.logger = logger or logging.getLogger(__name__)
        self.stats = {"processed": 0, "skipped": 0, "errors": 0}

    @abstractmethod
    def parse_stream(
        self, 
        stream: Union[TextIO, BinaryIO], 
        file_name: str, 
        sample_limit: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Parse a file stream, yielding record dicts.
        
        Args:
            stream: File-like object to read from (TextIO or BinaryIO)
            file_name: Name of the file being parsed (for metadata extraction)
            sample_limit: Optional maximum number of records to yield
            
        Returns:
            Iterator of dictionaries containing parsed records
        """
        raise NotImplementedError

    def parse_from_files(
        self, 
        files: List[str], 
        sample_limit: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Parse multiple files, handling both paths and streams.
        
        Args:
            files: Iterator of filenames or file objects
            sample_limit: Optional maximum number of records to yield
            
        Returns:
            Iterator of dictionaries containing parsed records
        """
        count = 0
        for file in files:
            close = isinstance(file, str)
            file_name = file if close else getattr(file, 'name', "unknown")
            
            stream = None
            try:
                # Let the parser handle the stream type it needs
                stream = file
                if close:
                    # Always use get_binary_stream or get_text_stream from file_utils directly
                    if file_name.endswith('.xml') or file_name.endswith('.xml.gz'):
                        stream = get_binary_stream(file_name, file_name)
                    else:
                        stream = get_text_stream(file_name, file_name)
                    
                for record in self.parse_stream(stream, file_name, sample_limit):
                    yield record
                    count += 1
                    self.stats["processed"] += 1
                    if sample_limit and count >= sample_limit:
                        return
                        
            except Exception as e:
                self.logger.error(f"Error processing file {file_name}: {str(e)}", exc_info=True)
                self.stats["errors"] += 1
            finally:
                if close and stream:
                    safe_close(stream)


# --- SQLDumpParser for SQL INSERT...VALUES parsing ---
class SQLDumpParser(BaseParser, ABC):
    """
    Abstract base parser for SQL dump INSERT...VALUES statements.
    Subclasses must implement target_table() and _process_tuple().
    """
    INSERT_RE = re.compile(
        r"INSERT INTO\s+`?(?P<table>\w+)`?.*?VALUES\s*(?P<values>.+);?$",
        re.IGNORECASE | re.DOTALL
    )
    TUPLE_RE = re.compile(r"\((?P<tuple>.*?)\)")

    def parse_stream(
        self,
        stream: Union[TextIO, BinaryIO],
        file_name: str,
        sample_limit: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        buffer = ""
        in_insert = False
        count = 0
        for line in stream:
            if sample_limit and count >= sample_limit:
                return
            line = line.strip()
            if not line or line.startswith("--"):
                continue
            if line.upper().startswith("INSERT INTO"):
                buffer = line
                in_insert = True
            elif in_insert:
                buffer += " " + line
            if in_insert and line.endswith(";"):
                m = self.INSERT_RE.search(buffer)
                if m and m.group("table") == self.target_table():
                    values_text = m.group("values")
                    for tup in self._split_tuples(values_text):
                        for rec in self._process_tuple(tup, file_name):
                            logging.debug(rec)
                            yield rec
                            count += 1
                            if sample_limit and count >= sample_limit:
                                return
                buffer = ""
                in_insert = False

    def _split_tuples(self, values_text):
        for m in self.TUPLE_RE.finditer(values_text):
            yield m.group("tuple")

    @abstractmethod
    def target_table(self):
        """Return the table name this parser handles."""
        raise NotImplementedError

    @abstractmethod
    def _process_tuple(self, tuple_text, file_name=None):
        """Convert a single tuple (raw text) into one or more records."""
        raise NotImplementedError