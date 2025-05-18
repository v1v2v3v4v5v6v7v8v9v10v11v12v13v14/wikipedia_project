import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
#from wiki_utils.compression import decompress_file
import requests
from wiki_utils.logging_utils import ApplicationLogger
from config.settings import OUTPUT_DIRECTORY_PATH
import processing.parser.maria_parser
from tqdm import tqdm


class FileDiscoverer:
    """
    Handles downloading of Wikipedia data dumps (pageviews, clickstream, revisions, pagelinks).
    Uses dependency injection for logging and external utilities for core operations.
    """

    LOG_DIR = Path(OUTPUT_DIRECTORY_PATH)/'logs'
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR = Path(OUTPUT_DIRECTORY_PATH)/'downloads'
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    BASE_URLS = {
        "pageviews": "https://dumps.wikimedia.org/other/pageview_complete/",
        "clickstream": "https://dumps.wikimedia.org/other/clickstream/",
        "revisions": "https://dumps.wikimedia.org/{wiki_code}/",
        "pagelinks": "https://dumps.wikimedia.org/{wiki_code}/",
    }

    def __init__(
        self,
        output_dir: Path = DOWNLOAD_DIR,
        logger: Optional[logging.Logger] = ApplicationLogger(log_dir=LOG_DIR).get_logger(),
        session: Optional[requests.Session] = None,
    ):
        """
        Initialize downloader with configured paths and dependencies.

        Args:
            output_dir: Base directory for downloaded files (will be expanded)
            logger: Pre-configured logger instance
            session: Optional pre-configured requests Session
        """
        self.output_dir = Path(output_dir).expanduser()
        self.output_dir_path = Path(output_dir)
        self.output_dir_path.mkdir(parents=True, exist_ok=True)
        self.logger = logger or logging.getLogger(__name__)
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "WikipediaDataDownloader/1.0"})


    def _discover_time_based_data(
        self,
        data_type: str,
        years: List[int],
        months: List[int],
        wiki_codes: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """Handle time-based datasets (pageviews, clickstream)"""
        stats = {"attempted": 0, "success": 0}

        for year in years:
            for month in months:
                ym = f"{year}-{month:02d}"
                url = self._build_time_based_url(data_type, ym)

                try:
                    # Find all files for this time period
                    all_files = self._find_remote_files(
                        data_type,
                        identifier=url,
                        dump_date=ym,
                        wiki_codes=wiki_codes  # Pass wiki_codes here to filter at the source
                    )
                    self.logger.info(f"Found {len(all_files)} {data_type} files for {ym}")

                    # Process each file only once
                    for filename in all_files:
                        stats["attempted"] += 1
                        if self._process_file(data_type, url, filename, year_month=ym, wiki_codes=wiki_codes):
                            stats["success"] += 1

                except requests.RequestException as e:
                    self.logger.error(
                        f"Failed {data_type} processing for {ym}: {str(e)}"
                    )

        return stats
    def _discover_wiki_based_data(
        self, dump_type: str, wiki_codes: List[str]
    ) -> Dict[str, int]:
        """Handle wiki-based datasets (revisions, pagelinks)"""
        stats = {"attempted": 0, "success": 0}
        for wiki_code in wiki_codes:
            try:
                dump_date = self._get_latest_dump_date(wiki_code)
                if not dump_date:
                    continue

                files = self._find_remote_files(
                    dump_type,
                    identifier=f"{self.BASE_URLS[dump_type].format(wiki_code=wiki_code)}{dump_date}/",
                    dump_date=dump_date,
                    wiki_codes=[wiki_code]
                )
                self.logger.info(
                    f"Found {len(files)} {dump_type} files for {wiki_code}"
                )

            except requests.RequestException as e:
                self.logger.error(
                    f"Failed {dump_type} processing for {wiki_code}: {str(e)}"
                )

        return stats

    # Helper Methods
    def _get_latest_dump_date(self, wiki_code: str) -> Optional[str]:
        """Find most recent dump date for a wiki project"""
        base_url = self.BASE_URLS["revisions"].format(wiki_code=wiki_code)
        dates = self._get_available_dates(base_url)
        if not dates:
            self.logger.warning(f"No dump dates found for {wiki_code}")
            return None

        dump_date = max(dates)
        self.logger.info(f"Latest dump date for {wiki_code}: {dump_date}")
        return dump_date

    def _get_available_dates(self, base_url: str) -> List[str]:
        """Scrape available YYYYMMDD dates from index page"""
        try:
            response = self.session.get(base_url, timeout=10)
            response.raise_for_status()
            return re.findall(r'href="(\d{8})/"', response.text)
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch dates from {base_url}: {str(e)}")
            return []

    def _build_time_based_url(self, data_type: str, year_month: str) -> str:
        """Construct appropriate URL for time-based data"""
        if data_type == "pageviews":
            return f"{self.BASE_URLS[data_type]}{year_month[:4]}/{year_month}/"
        return f"{self.BASE_URLS[data_type]}{year_month}/"

    def _find_remote_files(
        self,
        data_type: str,
        identifier: str,
        dump_date: Optional[str] = None,
        wiki_codes: Optional[List[str]] = None,
    ) -> List[str]:
        """Identify downloadable files from remote index"""
        # Fetch the index page
        response = self.session.get(identifier, timeout=10)
        response.raise_for_status()
        text = response.text
        matches: List[str] = []

        if data_type == "pageviews":
            pattern = r'href="(pageviews-\d{8}-[^"]+\.bz2)"'
            matches = re.findall(pattern, text)

        elif data_type in ["revisions", "pagelinks"]:
            for wiki_code in wiki_codes or []:
                if data_type == "revisions":
                    pattern = rf'href="/{wiki_code}/{dump_date}/({wiki_code}-{dump_date}-(?:stub-meta-history|stub-meta-current|stub-articles)\.xml\.gz)"'
                else:  # pagelinks
                    pattern = rf'href="/{wiki_code}/{dump_date}/({wiki_code}-{dump_date}-pagelinks\.sql\.gz)"'
                matches.extend(re.findall(pattern, text))

        elif data_type == "clickstream":
            for wiki_code in wiki_codes or []:
                pattern = rf'href="(clickstream-{wiki_code}-{dump_date}\.tsv\.gz)"'
                matches.extend(re.findall(pattern, text))

        return matches

    def _find_local_files(
        self,
        data_type: str,
        years: Optional[List[int]] = None,
        months: Optional[List[int]] = None,
        wiki_codes: Optional[List[str]] = None,
    ) -> List[str]:
        """Identify files already downloaded locally in the output directory"""
        base_path = self.output_dir / data_type
        if not base_path.exists():
            return []

        # Define filename patterns for each data type
        patterns: Dict[str, Any] = {
            "pageviews": re.compile(r"pageviews-\d{8}-[^/]+\.bz2$"),
            "clickstream": re.compile(r"clickstream-(?P<code>[a-z]+)-\d{4}-\d{2}\.tsv\.gz$"),
            "revisions": re.compile(r"(?P<code>[a-z]+)-\d{8}-(?:stub-meta-history|stub-meta-current|stub-articles)\.xml\.gz$"),
            "pagelinks": re.compile(r"(?P<code>[a-z]+)-\d{8}-pagelinks\.sql\.gz$"),
        }
        pattern = patterns.get(data_type)
        if pattern is None:
            return []

        matches: List[str] = []
        # Walk through all files under the data_type directory
        for path in base_path.rglob("*"):
            if not path.is_file():
                continue
            name = path.name
            m = pattern.match(name)
            if not m:
                continue
            # For types with wiki_codes, filter by provided codes if given
            if data_type in ["clickstream", "revisions", "pagelinks"] and wiki_codes:
                code = m.groupdict().get("code")
                if code not in wiki_codes:
                    continue
            matches.append(str(path))
        return matches

    def discover_files(
        self,
        data_type: str,
        years: Optional[List[int]] = None,
        months: Optional[List[int]] = None,
        wiki_codes: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """
        Unified download interface for all supported data types.

        Args:
            data_type: One of ['pageviews', 'clickstream', 'revisions', 'pagelinks']
            years: List of years to download (time-based data only)
            months: List of months to download (1-12, time-based data only)
            wiki_codes: List of wiki project codes (e.g., ['enwiki', 'dewiki'])

        Returns:
            Dictionary with download statistics {'attempted': X, 'success': Y}
        """
        if data_type in ["pageviews", "clickstream"]:
            return self._discover_time_based_data(
                data_type,
                years or [datetime.now().year],
                months or list(range(1, 13)),
                wiki_codes,
            )
        elif data_type in ["revisions", "pagelinks"]:
            self.logger.info(f"Wiki codes: {wiki_codes}")
            return self._discover_wiki_based_data(data_type, wiki_codes)
        raise ValueError(f"Unsupported data type: {data_type}")

    def discover_local_files(
        self,
        data_type: str,
        years: Optional[List[int]] = None,
        months: Optional[List[int]] = None,
        wiki_codes: Optional[List[str]] = None,
    ) -> List[str]:
        """
        List already-downloaded files for the given data_type.
 
        Args:
            data_type: One of ['pageviews', 'clickstream', 'revisions', 'pagelinks']
            years: List of years (for time-based data)
            months: List of months (for time-based data)
            wiki_codes: List of wiki project codes (for wiki-based data)
        Returns:
            List of file paths as strings
        """
        return self._find_local_files(data_type, years, months, wiki_codes)
    
    def discover_all_files(
        self,
        data_type: str,
        years: Optional[List[int]] = None,
        months: Optional[List[int]] = None,
        wiki_codes: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Return a combined list of available files: local file paths first (in full),
        then remote filenames (excluding any already present locally).
        """
        # Get local files and their basenames
        local_paths = self.discover_local_files(data_type, years, months, wiki_codes)
        self.logger.debug(f"local paths: {local_paths}")
        local_names = {str(Path(p).name) for p in local_paths}
        self.logger.debug(f"local_names: {local_names}")
        
        remote_names: List[str] = []
        # Time-based data (pageviews, clickstream)
        if data_type in ["pageviews", "clickstream"]:
            for year in (years or [datetime.now(timezone.utc).year]):
                for month in (months or list(range(1, 13))):
                    ym = f"{year}-{month:02d}"
                    url = self._build_time_based_url(data_type, ym)
                    files = self._find_remote_files(
                        data_type, identifier=url, dump_date=ym, wiki_codes=wiki_codes
                    )
                    for fname in files:
                        if fname not in local_names:
                            remote_names.append(fname)
        
        # Wiki-based data (revisions, pagelinks)
        elif data_type in ["revisions", "pagelinks"]:
            for code in (wiki_codes or []):
                dump_date = self._get_latest_dump_date(code)
                if not dump_date:
                    continue
                url = f"{self.BASE_URLS[data_type].format(wiki_code=code)}{dump_date}/"
                files = self._find_remote_files(
                    data_type, identifier=url, dump_date=dump_date, wiki_codes=[code]
                )
                for fname in files:
                    if fname not in local_names:
                        remote_names.append(fname)
        
        # Combine local and remote, with local full paths first
        return local_paths + remote_names
