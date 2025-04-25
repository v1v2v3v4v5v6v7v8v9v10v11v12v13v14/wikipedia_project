import re
from datetime import datetime, timezone
from typing import Optional
import logging

# Regex patterns for Wikimedia filename timestamps
FILENAME_PATTERNS = [
    # Pageview patterns
    r'pageviews-(\d{4})(\d{2})(\d{2})-\d{6}\.gz',
    r'pagecounts-(\d{4})-(\d{2})-(\d{2})-\d{6}\.gz',
    # Clickstream patterns
    r'clickstream-\w+-(\d{4})-(\d{2})\.tsv\.gz',
    # Revision dump patterns
    r'\w+-(\d{4})(\d{2})(\d{2})-\w+\.gz',
    # SQL dump patterns
    r'\w+-(\d{4})(\d{2})(\d{2})-\w+\.sql'
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
    if not timestamp_str:
        if logger:
            logger.debug(f"Empty timestamp in context: {context}")
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
        if logger:
            logger.error(f"Failed to parse timestamp '{timestamp_str}' from {context}: {str(e)}")
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
    for pattern in FILENAME_PATTERNS:
        match = re.search(pattern, filename)
        if match:
            groups = match.groups()
            if len(groups) == 3:  # YYYY, MM, DD
                return f"{groups[0]}-{groups[1]}-{groups[2]}"
            elif len(groups) == 2:  # YYYY, MM (clickstream)
                return f"{groups[0]}-{groups[1]}-01"  # Default to first day of month
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
    return ts.strftime("%Y-%m")
