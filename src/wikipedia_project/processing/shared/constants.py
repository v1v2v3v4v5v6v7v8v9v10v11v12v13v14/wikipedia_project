"""
Shared constants across the Wikipedia data processing pipeline.
Only for values used across multiple components.
"""

"""Namespace for pageview-specific constants"""
ACCESS_TYPES = {
    "desktop": 0,
    "mobile-app": 1,
    "mobile-web": 2,
    "all-access": 3
}

USER_TYPES = {
    "user": 0,
    "spider": 1,
    "bot": 2,
    "automated": 3,
    "all-agents": 4
}

# Mapping of source fields to internal TSV pageview fields to explicitly
# define source-to-internal field translation
TSV_PAGEVIEW_FIELD_MAPPING = {
    'wiki_code': 'wiki_code',
    'title': 'title',
    'access_type': 'access_type',
    'pageviews': 'pageviews',
    'hourly_views': 'hourly_views',
    'timestamp': 'timestamp',
    'page_hash': 'page_hash',
    'user_type': 'user_type'
}

# Field index mappings for TSV data
TSV_FIELD_INDICES = {
    'DOMAIN': 0,
    'TITLE': 1,
    'WIKI_INTERNAL_ID': 2,
    'ACCESS_TYPE': 3,
    'PAGEVIEWS': 4,
    'HOURLY_VIEWS': 5
}

# Line pattern classifications based on the number of fields
TSV_LINE_PATTERNS = {
    3: "domain | access_type | views",
    4: "domain | page_name | access_type | views",
    5: "domain | page_name | wiki_internal_id | access_type | views",
    6: "domain | page_name | wiki_internal_id | access_type | daily_views | hourly_views"
}

# Regex patterns for extracting timestamps from filenames
FILENAME_TIMESTAMP_PATTERNS = [
    r'pageviews-(\d{4})(\d{2})(\d{2})-(\d{6})',
    r'pageviews-(\d{4})(\d{2})(\d{2})',
    r'pagecounts-(\d{4})-(\d{2})-(\d{2})',
    r'pageviews-(\d{4})(\d{2})(\d{2})-user'
]

# Default numeric values for missing data
NUMERIC_DEFAULTS = {
    'ACCESS_TYPE': -1,
    'USER_TYPE': -1,
    'PAGEVIEWS': 0
}

# Reasons for filtering records during parsing
FILTER_REASONS = {
    'EMPTY_LINE': 'empty line encountered',
    'DISALLOWED_WIKI_CODE': 'disallowed wiki code',
    'EXCLUDED_PREFIX': 'excluded prefixes',
    'MISSING_HOURLY_VIEWS': 'missing hourly views data',
    'ZERO_PAGEVIEWS': 'total pageviews is zero or negative'
}

# Default prefixes used for various Wikipedia page types
DEFAULT_PREFIXES = {
    'Special:', 'Talk:', 'User:', 'User_talk:', 'Wikipedia:', 
    'Wikipedia_talk:', 'File:', 'File_talk:', 'MediaWiki:',
    'MediaWiki_talk:', 'Template:', 'Template_talk:', 'Help:',
    'Help_talk:', 'Category:', 'Category_talk:', 'Portal:',
    'Portal_talk:', 'Draft:', 'Draft_talk:', 'TimedText:',
    'TimedText_talk:', 'Module:', 'Module_talk:', 'Gadget:',
    'Gadget_talk:', 'Gadget_definition:', 'Gadget_definition_talk:'
}
