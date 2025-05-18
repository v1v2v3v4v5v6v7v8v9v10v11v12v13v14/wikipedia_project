"""Core parser for Wikipedia TSV data files."""
import logging
from collections import defaultdict, namedtuple
from datetime import datetime, timezone
from typing import Dict, Set, Optional, Any, Iterator, TextIO, BinaryIO, Union, List, Tuple
from urllib.parse import urlparse
from wikipedia_project.processing.parser.base_parser import BaseParser
from wikipedia_project.wiki_utils.datetime_utils import extract_date_from_filename, get_year_month_from_filename, normalize_timestamp_format
from wikipedia_project.wiki_utils.hashing_utils import WikimediaIdentifiers
from wikipedia_project.processing.shared.constants import DEFAULT_PREFIXES, ACCESS_TYPES, USER_TYPES, NUMERIC_DEFAULTS

DOMAIN_MAP = {
    "google.": "other-google",
    "yahoo.": "other-yahoo",
    "bing.": "other-bing",
    "facebook.": "other-facebook",
    "twitter.": "other-twitter",
}

class TSVParser(BaseParser):
    PageviewRecord = namedtuple('PageviewRecord', ['access_type', 'user_type', 'date', 'views'])
    
    def __init__(
        self,
        wiki_code: str,
        timestamp: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        excluded_prefixes: Optional[Set[str]] = None,
        default_prefixes: Set[str] = DEFAULT_PREFIXES
    ):
        super().__init__(logger)
        self.wiki_code = wiki_code
        self.timestamp = self._normalize_timestamp(timestamp) if timestamp else None
        self.excluded_prefixes = excluded_prefixes or default_prefixes
        self.filter_counts = defaultdict(int)
        self._last_updated = None
        
    def _normalize_timestamp(self, timestamp: str) -> Optional[str]:
        try:
            return normalize_timestamp_format(timestamp)
        except Exception as e:
            self.logger.warning(f"Failed to normalize timestamp '{timestamp}': {e}")
            return None

    def parse_stream(
        self,
        stream: Union[TextIO, BinaryIO],
        file_name: str,
        sample_limit: Optional[int] = None,
        wiki_filter: Optional[Set[str]] = None,
        batch_size: Optional[int] = None
    ) -> Union[Iterator[Dict[str, Any]], Iterator[List[Dict[str, Any]]]]:
        self._last_updated = None
        records_yielded = 0
        line_count = 0
        batch = []
        
        if not self.timestamp:
            self._set_timestamp_from_filename(file_name)

        for line_count, raw_line in enumerate(stream, 1):
            if line_count % 10000 == 0:
                self.logger.info(f"Processed {line_count} from {file_name} lines so far...")
                
            try:
                line = raw_line.strip()
                if not line:
                    continue

                if record := self._process_line(line, file_name, wiki_filter, line_count):
                    if batch_size is None:
                        yield record
                    else:
                        batch.append(record)
                        if len(batch) >= batch_size:
                            yield batch
                            batch = []

                    records_yielded += 1
                    self.stats["processed"] += 1

                    if sample_limit and records_yielded >= sample_limit:
                        break

            except Exception as e:
                self._handle_line_error(e, line_count)

        if batch_size and batch:
            yield batch

    def _process_line(self, line: str, filename: str, wiki_filter: Optional[Set[str]], line_count: int) -> Optional[Dict[str, Any]]:
        try:
            record = self.parse_line(line, filename, wiki_filter)
            if record:
                record["line_number"] = line_count
                return record
        except Exception as e:
            self._handle_line_error(e, line_count)
        return None

    def _handle_line_error(self, error: Exception, line_count: int) -> None:
        if isinstance(error, UnicodeDecodeError):
            self.logger.debug(f"Skipping undecodable binary line {line_count}")
        else:
            self.logger.warning(f"Failed to parse line {line_count}: {str(error)}", exc_info=True)
        self.stats["errors"] += 1

    def _set_timestamp_from_filename(self, filename: str) -> None:
        if extracted_date := extract_date_from_filename(filename):
            if normalized := normalize_timestamp_format(extracted_date):
                self.timestamp = normalized
                self.logger.info(f"Using timestamp from filename: {filename}")

    def parse_line(self, line: str, filename: str, wiki_filter: Optional[Set[str]] = None) -> Optional[Dict[str, Any]]:
        try:
            if 'clickstream' in filename:
                return self.parse_clickstream_line(line, filename, wiki_filter)
            return self.parse_pageview_line(line, filename, wiki_filter)
        except Exception as e:
            self.logger.error(f"Error parsing line: {str(e)}", exc_info=True)
            self.stats["errors"] += 1
            return None

    def parse_clickstream_line(self, line: str, filename: str, wiki_filter: Optional[Set[str]] = None) -> Optional[Dict[str, Any]]:
        if not line or len(parts := line.split('\t')) < 4:
            self.filter_counts["empty_lines" if not line else "invalid_format"] += 1
            return None

        source_page, target_page, raw_type, clicks_str = parts[:4]
        
        # Apply wiki filter if provided
        if wiki_filter is not None and self.wiki_code not in wiki_filter:
            self.filter_counts['wiki_code_not_allowed'] += 1
            return None

        clicks_val = int(clicks_str) if clicks_str.isdigit() else 0
        nav_type = raw_type if raw_type in ("link", "external", "other") else "other"
        
        record_date, year_month = self._get_record_dates(filename)
        page_hash = WikimediaIdentifiers.create_page_identifier(self.wiki_code, target_page)
        
        return {
            "page_hash": page_hash,
            "year_month": year_month,
            "wiki_code": self.wiki_code,
            "tuple_struct": {
                "target_page": target_page,
                "date": record_date or '',
                "clicks": clicks_val,
                "navigation_type": nav_type,
                "referer_category": self._get_referer_category(nav_type, source_page)
            }
        }

    def _get_record_dates(self, filename: str) -> Tuple[Optional[str], Optional[str]]:
        file_date = extract_date_from_filename(filename)
        year_month = get_year_month_from_filename(filename)
        if not file_date:
            return None, year_month
            
        try:
            dt_date = datetime.strptime(file_date, "%Y%m%d" if file_date.isdigit() else "%Y-%m-%d")
            return dt_date.strftime("%Y-%m-%d"), year_month
        except Exception:
            return file_date[:10], year_month

    def _get_referer_category(self, nav_type: str, source_page: str) -> str:
        if nav_type == "link":
            return "article"
        if nav_type == "external":
            domain = urlparse(source_page).netloc.lower()
            return next((v for k, v in DOMAIN_MAP.items() if k in domain), "other-other")
        return "other-empty"

    def parse_pageview_line(self, line: str, filename: str, wiki_filter: Optional[Set[str]] = None) -> Optional[Dict[str, Any]]:
        if not line or len(parts := line.split()) not in {3,4,5,6}:
            self.filter_counts["empty_lines" if not line else "invalid_format"] += 1
            return None

        domain, title, access_type, view_data, *_ = self._extract_pageview_fields(parts)
        if wiki_filter and domain not in wiki_filter:
            self.filter_counts['wiki_code_not_allowed'] += 1
            return None

        if title and self._should_exclude_page(title):
            self.filter_counts["excluded_prefix"] += 1
            return None

        if not (total_pageviews := int(view_data) if view_data.isdigit() else 0):
            self.filter_counts['zero_pageviews'] += 1
            return None

        timestamp_ms, dt = self._get_timestamp_data()
        year_month = dt.strftime("%Y-%m") if dt else None
        date_str = dt.strftime("%Y-%m-%d") if dt else None

        return {
            "page_hash": WikimediaIdentifiers.create_page_identifier(domain, title or ''),
            "title": title,
            "year_month": year_month,
            "wiki_code": domain,
            "tuple_struct": self.PageviewRecord(
                ACCESS_TYPES.get(access_type.lower(), NUMERIC_DEFAULTS['ACCESS_TYPE']),
                USER_TYPES.get(self._get_user_type(filename), NUMERIC_DEFAULTS['USER_TYPE']),
                dt,
                total_pageviews
            )
        }

    def _get_timestamp_data(self) -> Tuple[Optional[int], Optional[datetime]]:
        if not self.timestamp:
            return None, None
            
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y%m%d"):
            try:
                dt = datetime.strptime(self.timestamp, fmt).replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000), dt
            except ValueError:
                continue
        return None, None

    def _get_user_type(self, filename: str) -> str:
        lower_name = filename.lower()
        return ("user" if "-user" in lower_name else
                "spider" if "-spider" in lower_name else
                "automated" if any(x in lower_name for x in ("-automated", "-bot")) else
                "all")

    def _should_exclude_page(self, page_name: str) -> bool:
        if ':' in page_name and (prefix := page_name.split(':')[0]) in self.excluded_prefixes:
            self.filter_counts[f"prefix_{prefix}"] += 1
            return True
        return False

    def _extract_pageview_fields(self, parts: List[str]) -> Tuple[str, Optional[str], str, str, str]:
        field_map = {
            3: lambda p: (p[0], None, p[1], p[2], ''),
            4: lambda p: (p[0], p[1], p[2], p[3], ''),
            5: lambda p: (p[0], p[1], p[3], p[4], ''),
            6: lambda p: (p[0], p[1], p[3], p[4], p[5]),
        }
        if len(parts) not in field_map:
            raise ValueError("invalid_format")
        return field_map[len(parts)](parts)