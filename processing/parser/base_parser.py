from abc import ABC, abstractmethod
from typing import Iterator, TextIO, Dict, Any, Optional, Union, List
import logging

class BaseParser(ABC):
    """Abstract base class for data parsers implementing parse_stream."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize parser with optional logger."""
        self.logger = logger or logging.getLogger(__name__)

    @abstractmethod
    def parse_stream(self, stream: TextIO, file_name: str, sample_limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """
        Parse a file stream, yielding record dicts.
        
        Args:
            stream: File-like object to read from
            file_name: Name of the file being parsed (for metadata extraction)
            sample_limit: Optional maximum number of records to yield
            
        Returns:
            Iterator of dictionaries containing parsed records
        """
        raise NotImplementedError

    def parse_from_files(self, 
                         files: Iterator[Union[str, TextIO]], 
                         file_path: str, 
                         sample_limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """
        Parse multiple files, handling both paths and streams.
        
        Args:
            files: Iterator of filenames or file objects
            file_path: Path to the file(s) for reference
            sample_limit: Optional maximum number of records to yield
            
        Returns:
            Iterator of dictionaries containing parsed records
        """
        count = 0
        for file in files:
            close = isinstance(file, str)
            try:
                stream = open(file, encoding="utf-8") if close else file
                name = file if close else getattr(file, 'name', None)
                
                for record in self.parse_stream(stream, name, sample_limit):
                    yield record
                    count += 1
                    if sample_limit and count >= sample_limit:
                        return
            finally:
                if close and 'stream' in locals() and stream:
                    try:
                        stream.close()
                    except Exception as e:
                        self.logger.warning(f"Error closing file: {e}")