"""
Core parser for Wikipedia TSV data files
"""

import bz2
import gzip
import re
import os
from collections import defaultdict, Counter
from io import TextIOWrapper
from datetime import datetime, timezone
from pathlib import Path
import logging
from typing import Dict, Set, Optional, Any, Iterator, TextIO, List, Union
from icecream import ic

from processing.parser.base_parser import BaseParser
from processing.shared.constants import (
    DEFAULT_PREFIXES, 
    ACCESS_TYPES, 
    USER_TYPES, 
    NUMERIC_DEFAULTS,
    TSV_FIELD_INDICES,
    TSV_LINE_PATTERNS
)

class TSVParser(BaseParser):
    """Parser for TSV format Wikipedia data files."""
    
    def __init__(
        self,
        wiki_code: str,
        timestamp: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        excluded_prefixes: Optional[Set[str]] = None,
        default_prefixes: Set[str] = DEFAULT_PREFIXES
    ):
        """
        Initialize TSV parser.
        
        Args:
            wiki_code: Wiki identifier (e.g., 'enwiki')
            timestamp: Optional timestamp string
            logger: Optional custom logger
            excluded_prefixes: Set of prefixes to exclude
            default_prefixes: Default set of prefixes to exclude
        """
        super().__init__(logger)
        self.wiki_code = wiki_code
        self.timestamp = timestamp
        self.excluded_prefixes = excluded_prefixes or self._load_excluded_prefixes(default_prefixes)
        self.filter_counts = {
            'prefix': 0,
            'zero_views': 0,
            'wiki_filter': 0,
            'wiki_code_not_allowed': 0,
            'missing_hourly_views': 0,
            'zero_pageviews': 0
        }
        self._last_updated = None
        
    def parse_stream(
        self,
        stream_or_path: Any,
        file_name: str,
        sample_limit: Optional[int] = None,
        wiki_filter: Optional[Set[str]] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Parse a TSV file stream yielding parsed records.
        
        Args:
            stream_or_path: File-like object or path to file
            file_name: Name of the file being parsed
            sample_limit: Optional maximum number of records to yield
            wiki_filter: Optional set of wiki codes to include
            
        Returns:
            Iterator of dictionaries containing parsed records
        """
        self._last_updated = None
        self.timestamp = self.get_timestamp_from_filename(file_name)

        # Handle file-like object or path
        binary_stream = self._get_binary_stream(stream_or_path, file_name)
        text_stream = TextIOWrapper(binary_stream, encoding="utf-8", errors="replace")
        
        try:
            records_yielded = 0
            line_count = 0

            for raw_line in text_stream:
                line_count += 1
                try:
                    line = raw_line.strip()
                    if not line:
                        continue

                    record = ic(self.parse_line(line, file_name, wiki_filter))
                    if record:
                        record["file_name"] = file_name
                        record["line_number"] = line_count

                        yield record
                        records_yielded += 1
                        if sample_limit and records_yielded >= sample_limit:
                            break

                except Exception as e:
                    self.logger.warning(
                        f"Failed to parse line {line_count} from {file_name}: {str(e)}",
                        exc_info=True
                    )
                    continue
        finally:
            # Close stream (binary_stream will be closed by text_stream)
            try:
                text_stream.close()
            except Exception as e:
                self.logger.warning(f"Error closing stream: {e}")

    def _get_binary_stream(self, stream_or_path: Any, file_name: str) -> Any:
        """
        Get binary stream from file object or path.
        
        Args:
            stream_or_path: File-like object or path to file
            file_name: Name of the file for determining compression
            
        Returns:
            Binary stream
        """
        # Determine if we need to open a file
        if not hasattr(stream_or_path, "read"):
            file_obj = open(stream_or_path, "rb")
        else:
            file_obj = stream_or_path
            
        # Handle compression based on file extension
        if file_name.endswith(".bz2"):
            return bz2.BZ2File(file_obj, "rb")
        elif file_name.endswith(".gz"):
            return gzip.GzipFile(fileobj=file_obj, mode="rb")
        else:
            return file_obj

    def parse_line(self, line: str, filename: str, wiki_filter: Optional[Set[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Parse single line of TSV data.
        
        Args:
            line: Single line from TSV file
            filename: Name of source file
            wiki_filter: Optional set of wiki codes to include
            
        Returns:
            Dictionary with parsed data or None if filtered out
        """
        try:
            if 'clickstream' in filename:
                return self.parse_clickstream_line(line, filename)
            return self.parse_pageview_line(line, filename, wiki_filter)
        except Exception as e:
            self.logger.error(f"Error parsing line: {str(e)}", exc_info=True)
            return None

    def parse_clickstream_line(self, line: str, filename: str) -> Optional[Dict[str, Any]]:
        """
        Parse a line from the clickstream TSV data.
        
        Args:
            line: Single line from clickstream TSV file
            filename: Name of source file
            
        Returns:
            Dictionary with parsed data or None if filtered out
        """
        if not line.strip():
            self.logger.debug("Filtered out record (reason: empty line encountered)")
            return None

        parts = line.strip().split('\t')  # Clickstream uses tab separation
        if len(parts) < 4:  # Clickstream format: source target type count
            self.logger.debug("Filtered out clickstream record (reason: not enough fields)")
            return None

        try:
            source_page = parts[0]
            target_page = parts[1]
            clicks_str = parts[3]
            
            ts_str = self.get_timestamp_from_filename(filename)
            if ts_str:
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                timestamp = int(dt.timestamp() * 1000)
            else:
                timestamp = None

            return {
                "wiki_code": self.wiki_code,
                "source_page": source_page,
                "target_page": target_page,
                "clicks": int(clicks_str) if clicks_str.isdigit() else 0,
                "timestamp": timestamp
            }
        except Exception as e:
            self.logger.error(f"Error parsing clickstream line: {str(e)}", exc_info=True)
            return None

    def parse_pageview_line(
            self, 
            line: str, 
            filename: str,
            wiki_filter: Optional[Set[str]] = None,
        ) -> Optional[Dict[str, Any]]:
        """
        Parse a pageview record line into structured data.
        
        Args:
            line: Raw pageview record line
            filename: Name of source file
            wiki_filter: Set of allowed wiki codes (None means all allowed)
            
        Returns:
            Dictionary with parsed data or None if record should be filtered/ignored
        """
        if not line.strip():
            self.logger.debug("Filtered out record (reason: empty line encountered)")
            return None

        parts = line.strip().split()
        
        # Handle different TSV formats based on field count
        if len(parts) == 3:
            domain, access_type, view_data = parts
            title = None
            hourly_views_str = ''
        elif len(parts) == 4:
            domain, title, access_type, view_data = parts
            hourly_views_str = ''
        elif len(parts) == 5:  # "domain | page_name | wiki internal id | access_type | views"
            domain, title, _, access_type, view_data = parts
            hourly_views_str = ''
        elif len(parts) == 6:  # "domain | page_name | wiki internal id | access_type | daily_views | hourly_views"
            domain, title, _, access_type, view_data, hourly_views_str = parts
        else:
            self.logger.debug(f"Filtered out record (reason: unexpected number of fields {len(parts)}): {line.strip()}")
            return None

        # Extract and validate wiki code
        wiki_code = self.wiki_code
        if wiki_filter is not None:
            if wiki_code not in wiki_filter:
                self.filter_counts['wiki_code_not_allowed'] += 1
                if self.filter_counts['wiki_code_not_allowed'] % 100000 == 0:
                    self.logger.debug(f"Filtered out record (reason: disallowed wiki code '{wiki_code}'): {line.strip()}")
                return None

        # Analyze page name if present
        if title:
            page_prefixes = Counter()
            special_formats = defaultdict(list)
            self._analyze_page_name(title, page_prefixes, special_formats)
            
            # Skip excluded pages (special pages, user pages, etc.)
            if page_prefixes:  # If any prefixes were found
                self.logger.debug(f"Filtered out record (reason: excluded prefixes '{list(page_prefixes.keys())}'): {title}")
                return None

        # Extract raw hourly views string using regex pattern
        hourly_views_str = ''
        if len(parts) > 4:
            hourly_views_str_candidate = parts[-1]
            if re.fullmatch(r'(?:[A-Za-z]\d{1,2})+', hourly_views_str_candidate):
                hourly_views_str = hourly_views_str_candidate

        if not hourly_views_str:
            self.filter_counts['missing_hourly_views'] += 1
            if self.filter_counts['missing_hourly_views'] % 100000 == 0:
                self.logger.debug(f"Filtered out record (reason: missing hourly views data in record): {line.strip()}")
            return None

        # Determine total pageviews
        total_pageviews = int(view_data) if view_data.isdigit() else 0

        # Skip records with no views
        if total_pageviews <= 0:
            self.filter_counts['zero_pageviews'] += 1
            if self.filter_counts['zero_pageviews'] % 100000 == 0:
                self.logger.debug(f"Filtered out record (reason: total pageviews is zero or negative): {line.strip()}")
            return None

        # Extract and map access and user types
        numeric_access_type = ACCESS_TYPES.get(access_type.lower(), NUMERIC_DEFAULTS['ACCESS_TYPE'])
        user_type_str = self.extract_user_type_from_filename(filename) if filename else 'all'
        numeric_user_type = USER_TYPES.get(user_type_str.lower(), NUMERIC_DEFAULTS['USER_TYPE'])

        if not title:
            self.logger.error(f"Error: Empty title encountered in line '{line.strip()}' from file '{filename}'")
            raise ValueError("Empty page title encountered, record skipped.")

        # Get timestamp
        timestamp_str = self.timestamp
        if timestamp_str:
            timestamp_ms = int(datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                              .replace(tzinfo=timezone.utc).timestamp() * 1000)
        else:
            timestamp_ms = None

        return {
            'wiki_code': domain,
            'title': title,
            'access_type': numeric_access_type,
            'views': total_pageviews,
            'hourly_views': hourly_views_str,
            'timestamp': timestamp_ms,
            'page_hash': title,
            'user_type': numeric_user_type
        }

    @staticmethod
    def _analyze_page_name(page_name: str, page_prefixes: Counter, special_formats: Dict[str, List[str]]) -> None:
        """
        Analyze page names to identify prefixes and special formats.
        
        Args:
            page_name: Page title to analyze
            page_prefixes: Counter to track prefix occurrences
            special_formats: Dictionary to collect special format examples
        """
        if ':' in page_name:
            prefix = page_name.split(':')[0]
            page_prefixes[prefix] += 1
            special_formats[prefix].append(page_name)
        elif page_name.startswith('Special:'):
            page_prefixes['Special'] += 1
            special_formats['Special'].append(page_name)

    @staticmethod
    def _load_excluded_prefixes(defaults: Set[str]) -> Set[str]:
        """
        Load excluded prefixes from env or use defaults.
        
        Args:
            defaults: Default set of prefixes
            
        Returns:
            Set of prefixes to exclude
        """
        prefixes_env = os.environ.get("PREFIXES", "")
        return {
            prefix.strip() 
            for prefix in prefixes_env.split(',') 
            if prefix.strip()
        } if prefixes_env else defaults

    def extract_user_type_from_filename(self, filename: str) -> str:
        """
        Parse the filename to identify and return the user type.
        
        Args:
            filename: Name of source file
            
        Returns:
            User type: 'user', 'spider', 'automated', or 'all'
        """
        lower_filename = filename.lower()
        if "-user" in lower_filename:
            return "user"
        elif "-spider" in lower_filename:
            return "spider"
        elif "-automated" in lower_filename or "-bot" in lower_filename:
            return "automated"
        return "all"

    def get_timestamp_from_filename(self, filename: str) -> Optional[str]:
        """
        Extract timestamp from filename.
        
        Args:
            filename: Name of source file
            
        Returns:
            Timestamp string in format "%Y-%m-%d %H:%M:%S" or None
        """
        path = Path(filename)
        name = path.name

        # 1. Match YYYYMMDD in filename (e.g., pageview-20250420)
        m8 = re.search(r'(\d{8})', name)
        if m8:
            y, m, d = m8.group(1)[:4], m8.group(1)[4:6], m8.group(1)[6:8]
            return f"{y}-{m}-{d} 00:00:00"

        # 2. Match YYYY-MM in filename (e.g., clickstream-enwiki-2024-01.tsv)
        m_ym = re.search(r'(\d{4})-(\d{2})', name)
        if m_ym:
            y, mo = m_ym.group(1), m_ym.group(2)
            return f"{y}-{mo}-01 00:00:00"

        # 3. Match YYYYMM directory in path (e.g., /.../202401/)
        ym_folder = next((p for p in path.parts if re.fullmatch(r'\d{6}', p)), None)
        if ym_folder:
            y, mo = ym_folder[:4], ym_folder[4:6]
            return f"{y}-{mo}-01 00:00:00"

        return None