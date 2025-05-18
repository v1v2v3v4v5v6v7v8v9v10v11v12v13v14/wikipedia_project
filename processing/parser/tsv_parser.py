"""
Core parser for Wikipedia TSV data files
"""

import logging
import io
from collections import defaultdict, Counter, namedtuple
from datetime import datetime, timezone
from typing import Dict, Set, Optional, Any, Iterator, TextIO, BinaryIO, Union, List
from icecream import ic
from processing.shared.file_utils import get_text_stream
from processing.parser.base_parser import BaseParser
from wiki_utils.datetime_utils import extract_date_from_filename, normalize_timestamp_format
from urllib.parse import urlparse
from wiki_utils.hashing_utils import WikimediaIdentifiers
from processing.shared.constants import (
    DEFAULT_PREFIXES,
    ACCESS_TYPES,
    USER_TYPES,
    NUMERIC_DEFAULTS
)
class TSVParser(BaseParser):
    PageviewRecord = namedtuple('PageviewRecord', ['access_type', 'user_type', 'date', 'views'])
    """Parser for TSV format Wikipedia data files."""
    
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
        if timestamp:
            try:
                self.timestamp = normalize_timestamp_format(timestamp)
            except Exception as e:
                self.logger.warning(f"Failed to normalize timestamp '{timestamp}': {e}")
                self.timestamp = None
        else:
            self.timestamp = None
        self.excluded_prefixes = excluded_prefixes or default_prefixes
        self.filter_counts = defaultdict(int)
        self._last_updated = None
        
    def parse_stream(
        self,
        stream: Union[TextIO, BinaryIO],
        file_name: str,
        sample_limit: Optional[int] = None,
        wiki_filter: Optional[Set[str]] = None,
        batch_size: Optional[int] = None
    ) -> Union[Iterator[Dict[str, Any]], Iterator[List[Dict[str, Any]]]]:
        """Parse stream yielding either individual records or batches.
        
        Args:
            stream: Input stream to parse
            file_name: Source filename for metadata
            sample_limit: Max records to process
            wiki_filter: Set of allowed wiki codes
            batch_size: If None yields single records, else yields batches of this size
            
        Returns:
            Iterator of records (if batch_size=None) or batches of records
        """
        self._last_updated = None
        records_yielded = 0
        line_count = 0
        batch = []

        if not self.timestamp:
            if extracted_date := extract_date_from_filename(file_name):
                if normalized := normalize_timestamp_format(extracted_date):
                    self.timestamp = normalized
                    self.logger.info(f"Using timestamp from filename: {file_name} at line {line_count}")

        try:
            with get_text_stream(stream, file_name) as text_stream:
                for raw_line in text_stream:
                    line_count += 1
                    if line_count % 10000 == 0:
                        self.logger.info(f"Processed {line_count} from {file_name} lines so far...")
                    try:
                        line = raw_line.decode('utf-8').strip() if isinstance(raw_line, bytes) else raw_line.strip()
                        if not line:
                            continue

                        if record := self.parse_line(line, file_name, wiki_filter):
                            record.update({
                                "line_number": line_count
                            })
                            
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

                    except UnicodeDecodeError:
                        self.logger.debug(f"Skipping undecodable binary line {line_count}")
                        self.stats["errors"] += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to parse line {line_count}: {str(e)}", exc_info=True)
                        self.stats["errors"] += 1

                if batch_size and batch:  # Yield final partial batch
                    yield batch

        except Exception as e:
            self.logger.error(f"Stream read error: {str(e)}", exc_info=True)
            raise        
    def parse_line(self, line: str, filename: str, wiki_filter: Optional[Set[str]] = None) -> Optional[Dict[str, Any]]:
        try:
            if 'clickstream' in filename:
                return self.parse_clickstream_line(line, filename)
            return self.parse_pageview_line(line, filename, wiki_filter)
        except Exception as e:
            self.logger.error(f"Error parsing line: {str(e)}", exc_info=True)
            self.stats["errors"] += 1
            return None

    def parse_clickstream_line(self, line: str, filename: str) -> Optional[Dict[str, Any]]:
        if not line:
            self.filter_counts["empty_lines"] += 1
            return None

        parts = line.split('\t')
        if len(parts) < 4:
            self.filter_counts["invalid_format"] += 1
            return None

        try:
            source_page = parts[0]
            target_page = parts[1]
            clicks_str = parts[3]
            
            timestamp = None
            if self.timestamp:
                try:
                    dt = datetime.strptime(self.timestamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    timestamp = int(dt.timestamp() * 1000)
                except ValueError as e:
                    self.logger.warning(f"Invalid timestamp format '{self.timestamp}': {str(e)}")

            # derive year_month from filename
            year_month = None
            if (file_date := extract_date_from_filename(filename)):
                try:
                    # handle YYYYMMDD or YYYY-MM-DD formats
                    if file_date.isdigit() and len(file_date) == 8:
                        dt_obj = datetime.strptime(file_date, "%Y%m%d")
                    else:
                        dt_obj = datetime.strptime(file_date, "%Y-%m-%d")
                    year_month = dt_obj.strftime("%Y-%m")
                except Exception:
                    # fallback: take first 7 characters
                    year_month = file_date[:7]

            # derive date from filename
            record_date = None
            if file_date:
                try:
                    if file_date.isdigit() and len(file_date) == 8:
                        dt_date = datetime.strptime(file_date, "%Y%m%d")
                    else:
                        dt_date = datetime.strptime(file_date, "%Y-%m-%d")
                    record_date = dt_date.strftime("%Y-%m-%d")
                except Exception:
                    record_date = file_date[:10]

            clicks_val = int(clicks_str) if clicks_str.isdigit() else 0

            # Classify navigation type
            raw_type = parts[2]
            nav_type = raw_type if raw_type in ("link", "external", "other") else "other"

            # Determine referer category
            parsed_url = urlparse(source_page)
            if nav_type == "link":
                referer_category = "article"
            elif nav_type == "external":
                domain = parsed_url.netloc.lower()
                if "google." in domain:
                    referer_category = "other-google"
                elif "yahoo." in domain:
                    referer_category = "other-yahoo"
                elif "bing." in domain:
                    referer_category = "other-bing"
                elif "facebook." in domain:
                    referer_category = "other-facebook"
                elif "twitter." in domain:
                    referer_category = "other-twitter"
                else:
                    referer_category = "other-other"
            else:
                referer_category = "other-empty"

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
                    "referer_category": referer_category
                }
            }
        except Exception as e:
            self.logger.error(f"Error parsing clickstream line: {str(e)}", exc_info=True)
            self.stats["errors"] += 1
            return None
        

    def parse_pageview_line(
            self, 
            line: str, 
            filename: str,
            wiki_filter: Optional[Set[str]] = None,
        ) -> Optional[Dict[str, Any]]:
        if not line:
            self.filter_counts["empty_lines"] += 1
            return None

        parts = line.split()
        
        if len(parts) == 3:
            domain, access_type, view_data = parts
            title = None
            hourly_views_str = ''
        elif len(parts) == 4:
            domain, title, access_type, view_data = parts
            hourly_views_str = ''
        elif len(parts) == 5:
            domain, title, _, access_type, view_data = parts
            hourly_views_str = ''
        elif len(parts) == 6:
            domain, title, _, access_type, view_data, hourly_views_str = parts
        else:
            self.filter_counts["invalid_format"] += 1
            return None

        wiki_code = domain
        if wiki_filter is not None and wiki_code not in wiki_filter:
            self.filter_counts['wiki_code_not_allowed'] += 1
            return None

        if title and self._should_exclude_page(title):
            self.filter_counts["excluded_prefix"] += 1
            return None

        total_pageviews = int(view_data) if view_data.isdigit() else 0
        if total_pageviews <= 0:
            self.filter_counts['zero_pageviews'] += 1
            return None

        numeric_access_type = ACCESS_TYPES.get(access_type.lower(), NUMERIC_DEFAULTS['ACCESS_TYPE'])
        user_type_str = self.extract_user_type_from_filename(filename) if filename else 'all'
        numeric_user_type = USER_TYPES.get(user_type_str.lower(), NUMERIC_DEFAULTS['USER_TYPE'])

        timestamp_ms = None
        dt = None
        if self.timestamp:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y%m%d"):
                try:
                    dt = datetime.strptime(self.timestamp, fmt).replace(tzinfo=timezone.utc)
                    timestamp_ms = int(dt.timestamp() * 1000)
                    break
                except ValueError:
                    continue
            if dt is None:
                self.logger.warning(f"Invalid timestamp format '{self.timestamp}'")

        # derive date (YYYY-MM-DD) from timestamp_ms
        date_str = None
        if timestamp_ms is not None:
            dt_date = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            date_str = ic(dt_date.strftime("%Y-%m-%d"))

        # Extract and set year_month field
        year_month = None
        if dt is not None:
            year_month = dt.strftime("%Y-%m")

        page_hash = WikimediaIdentifiers.create_page_identifier(domain, title or '')
        return {
            "page_hash": page_hash,
            "title": title,
            "year_month": year_month,
            "wiki_code": domain,
            "tuple_struct": self.PageviewRecord(numeric_access_type, numeric_user_type, dt, total_pageviews)
            }

    def save_pageview(self, pageview_data: Any) -> bool:
        """
        Save a PageView record with nested structure.
        Expects data in the format output by parse_pageview_line.
        """
        try:
            # Extract the key fields
            page_hash = pageview_data.get("page_hash")
            if not page_hash:
                self.logger.warning("No page_hash found in pageview data")
                return False
            
            # For testing purposes, skip validation or always return True
            # In production, uncomment the following:
            # if not self._validate_document_exists(page_hash):
            #    self.logger.warning(f"Document {page_hash} not found for pageview")
            #    return False
            
            # Extract tuple_struct which contains our nested data
            tuple_struct = pageview_data.get("tuple_struct", {})
            if not tuple_struct:
                self.logger.warning("No tuple_struct found in pageview data")
                return False
            
            # Extract the fields from tuple_struct
            numeric_access_type = tuple_struct.get("access_type", 0)
            numeric_user_type = tuple_struct.get("user_type", 0)
            date_str = tuple_struct.get("date", "")
            views = tuple_struct.get("views", 0)
            
            # Extract or determine year_month
            year_month = pageview_data.get("year_month")
            if not year_month and date_str:
                # Try to derive year_month from date if not provided
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    year_month = dt.strftime("%Y-%m")
                except ValueError:
                    # If we can't parse the date, use current year-month as fallback
                    year_month = datetime.now().strftime("%Y-%m")
            
            # If we still don't have year_month, use current
            if not year_month:
                year_month = datetime.now().strftime("%Y-%m")
            
            # For testing, directly insert the whole record into PAGEVIEW
            self.PAGEVIEW.insert_one(pageview_data)
            
            # Also update the page bucket if you're using that approach
            # This will add the data to the unified structure
            if hasattr(self, 'PAGES') and callable(getattr(self, '_upsert_page_bucket', None)):
                self._upsert_page_bucket(
                    page_hash,
                    year_month,
                    {"$inc": {f"pageviews.{numeric_access_type}.{numeric_user_type}.{date_str}": views}}
                )
                self.logger.info(f"Pageview updated for {page_hash} on {date_str}.")
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to save pageview: {e}")
            return False

    def _should_exclude_page(self, page_name: str) -> bool:
        if ':' in page_name:
            prefix = page_name.split(':')[0]
            if prefix in self.excluded_prefixes:
                self.filter_counts[f"prefix_{prefix}"] += 1
                return True
        return False

    def extract_user_type_from_filename(self, filename: str) -> str:
        lower_filename = filename.lower()
        if "-user" in lower_filename:
            return "user"
        elif "-spider" in lower_filename:
            return "spider"
        elif "-automated" in lower_filename or "-bot" in lower_filename:
            return "automated"
        return "all"