# wiki_utils/datetime_utils.py
import re
from datetime import datetime, timezone
from typing import Optional, Union
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Regex patterns for Wikimedia filename timestamps
FILENAME_PATTERNS = [
    # Pageview patterns
    r'pageviews-(\d{4})(\d{2})(\d{2})(?:-[a-z]+)?\.bz2',
    r'pagecounts-(\d{4})-(\d{2})-(\d{2})-\d{6}\.gz',
    # Clickstream patterns - updated to correctly match the format
    r'clickstream-\w+-(\d{4})-(\d{2})\.tsv(?:\.gz)?',
    # Revision dump patterns
    r'\w+-(\d{4})(\d{2})(\d{2})-\w+\.gz',
    # SQL dump patterns
    r'\w+-(\d{4})(\d{2})(\d{2})-\w+\.sql',
    # Directory patterns
    r'\/(\d{4})(\d{2})\/.*'
]

def parse_wikimedia_timestamp(
    timestamp_str: Optional[str], 
    context: str = "unknown",
    logger: Optional[logging.Logger] = None
) -> Optional[datetime]:
    """
    Parse Wikimedia timestamp string into timezone-aware UTC datetime.
    
    Args:
        timestamp_str: Input string in ISO 8601 format (may end with 'Z')
        context: Description of where timestamp came from for error messages
        logger: Optional logger for error reporting
        
    Returns:
        timezone-aware datetime in UTC or None if parsing failed
    """
    log = logger or logging.getLogger(__name__)
    
    if not timestamp_str:
        log.debug(f"Empty timestamp in context: {context}")
        return None

    try:
        # Convert 'Z' suffix to explicit UTC offset
        normalized = timestamp_str.replace('Z', '+00:00') if timestamp_str.endswith('Z') else timestamp_str
        dt = datetime.fromisoformat(normalized)
        
        # Ensure timezone is UTC
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
        
    except ValueError as e:
        log.error(f"Failed to parse timestamp '{timestamp_str}' from {context}: {str(e)}")
        return None

def extract_date_from_filename(filename: str) -> Optional[str]:
    """
    Extract YYYY-MM-DD date from Wikimedia dump filenames.
    
    Supports:
    - pageviews-YYYYMMDD-HHMMSS.gz
    - pagecounts-YYYY-MM-DD-HHMMSS.gz
    - clickstream-wiki-YYYY-MM.tsv.gz
    - wiki-YYYYMMDD-stub-meta-history.gz
    
    Returns:
        Date string in YYYY-MM-DD format or None if no match
    """
    path = Path(filename) if not isinstance(filename, Path) else filename
    path_str = str(path)
    
    for pattern in FILENAME_PATTERNS:
        match = re.search(pattern, path_str)
        if match:
            groups = match.groups()
            if len(groups) == 3:  # YYYY, MM, DD
                return f"{groups[0]}-{groups[1]}-{groups[2]}"
            elif len(groups) == 2:  # YYYY, MM (clickstream)
                return f"{groups[0]}-{groups[1]}-01"  # Default to first day of month
    
    return None

def get_year_month_from_filename(filename: str) -> Optional[str]:
    """Return the ``YYYY-MM`` year-month portion from Wikimedia dump filenames.

    This helper mirrors :func:`extract_date_from_filename` but returns only the
    year and month components when a date is present.

    Args:
        filename: The path or name of the dump file.

    Returns:
        A string in ``YYYY-MM`` format if a date could be parsed, otherwise
        ``None``.
    """
    path = Path(filename) if not isinstance(filename, Path) else filename
    path_str = str(path)

    for pattern in FILENAME_PATTERNS:
        match = re.search(pattern, path_str)
        if match:
            groups = match.groups()
            if len(groups) >= 2:  # YYYY, MM at minimum
                return f"{groups[0]}-{groups[1]}"

    return None

def convert_to_utc(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware and in UTC"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def format_as_iso(dt: datetime) -> str:
    """Format datetime as ISO 8601 with 'Z' suffix"""
    return convert_to_utc(dt).isoformat(timespec='seconds').replace('+00:00', 'Z')

def make_year_month(ts: datetime) -> str:
    """Format datetime as YYYY-MM string"""
    return ts.strftime("%Y-%m")

def normalize_timestamp_format(timestamp: Optional[Union[str, int, float, datetime]]) -> Optional[str]:
    """
    Normalize various timestamp inputs into a standard timestamp string.
    
    Args:
        timestamp: Input timestamp, which may be:
          - int or float (milliseconds since epoch)
          - datetime (naive or timezone-aware)
          - str in various date or datetime formats
    
    Returns:
        A timestamp string in `YYYY-MM-DD HH:MM:SS` format, or `None` if input was `None` or invalid.
    """
    if timestamp is None:
        return None

    # Integer or float: interpret as milliseconds since epoch
    if isinstance(timestamp, (int, float)):
        dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    # Already a datetime
    if isinstance(timestamp, datetime):
        dt = timestamp
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    # String: normalize formats then return string
    ts = timestamp
    # Already has date+time
    if ' ' in ts and len(ts) >= 19:
        return ts[:19]
    # YYYY-MM-DD
    if len(ts) == 10 and ts[4] == '-' and ts[7] == '-':
        return f"{ts} 00:00:00"
    # YYYY-MM
    if len(ts) == 7 and ts[4] == '-':
        return f"{ts}-01 00:00:00"
    # YYYYMMDD
    if len(ts) == 8 and ts.isdigit():
        return f"{ts[:4]}-{ts[4:6]}-{ts[6:8]} 00:00:00"

    raise ValueError(f"Cannot normalize timestamp string: {ts}")