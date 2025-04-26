"""
Utilities for downloading Wikimedia dump files
"""

import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import requests
from tqdm import tqdm
import logging
import re
from icecream import ic
import gzip
import bz2
import shutil



class WikimediaDownloader:
    """Handles downloading of various Wikimedia data types"""
    
    BASE_URLS = {
        "pageviews": "https://dumps.wikimedia.org/other/pageview_complete/",
        "clickstream": "https://dumps.wikimedia.org/other/clickstream/",
        "revisions": "https://dumps.wikimedia.org/{wiki_code}/",
        "pagelinks": "https://dumps.wikimedia.org/{wiki_code}/",
    }
    
    def __init__(self, output_dir: Path, logger: Optional[logging.Logger] = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logger or logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'WikimediaDownloader/1.0'})

    def download(
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
            return self._download_time_based_data(
                data_type,
                years or [datetime.now().year],
                months or list(range(1, 13)),
                wiki_codes,
            )
        elif data_type in ["revisions", "pagelinks"]:
            self.logger.info(f"Wiki codes: {wiki_codes}")
            return self._download_wiki_based_data(data_type, wiki_codes)
        raise ValueError(f"Unsupported data type: {data_type}")

    def _download_time_based_data(
        self, 
        data_type: str, 
        years: List[int], 
        months: List[int],
        wiki_codes: Optional[List[str]] = None
    ) -> Dict[str, int]:
        """Download time-based data like pageviews and clickstream"""
        stats = {"attempted": 0, "success": 0}
        
        for year in years:
            for month in months:
                year_str = str(year)
                month_str = ic(f"{month:02d}")
                year_month = ic(f"{year_str}-{month_str}")
                
                # Build URL
                if data_type == "pageviews":
                    url = ic(f"{self.BASE_URLS[data_type]}{year_str}/{year_month}/")
                elif data_type == "clickstream":
                    url = ic(f"{self.BASE_URLS[data_type]}{year_month}/")
                else:
                    raise ValueError(f"Unknown time-based data type: {data_type}")
                
                # Get file list
                try:
                    response = ic(self.session.get(url, timeout=10))
                    response.raise_for_status()
                    ic(f'href="({data_type}-.*?{wiki_codes[0]}.*?\.gz)"')
                    ic(response.text)
                    # Filter files based on wiki_codes if provided
                    files = []
                    if wiki_codes:
                        for code in wiki_codes:
                            files.extend(re.findall(f'href="({data_type}-.*?{code}.*?\.gz)"', response.text))
                    else:
                        files = ic(re.findall(f'href="({data_type}-.*?\.gz)"', response.text))
                    
                    for filename in files:
                        stats["attempted"] += 1
                        if self._process_file(data_type, url, filename, year_month, wiki_codes=wiki_codes):
                            stats["success"] += 1
                
                except requests.RequestException as e:
                    self.logger.error(f"Error fetching file list for {url}: {str(e)}")
        
        return stats
    

    def decompress_file(file_path: Path, logger: Optional[logging.Logger] = None) -> bool:
        """
        Decompresses a .gz or .bz2 file in place (replaces compressed file with decompressed version).
        """
        try:
            if file_path.suffix == ".gz":
                decompressed_path = file_path.with_suffix('')
                with gzip.open(file_path, 'rb') as f_in, open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                file_path.unlink()  # remove original .gz
                if logger:
                    logger.info(f"Decompressed {file_path} -> {decompressed_path}")
                return True

            elif file_path.suffix == ".bz2":
                decompressed_path = file_path.with_suffix('')
                with bz2.open(file_path, 'rb') as f_in, open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                file_path.unlink()  # remove original .bz2
                if logger:
                    logger.info(f"Decompressed {file_path} -> {decompressed_path}")
                return True

            else:
                if logger:
                    logger.warning(f"Unsupported compression format: {file_path.suffix}")
                return False

        except Exception as e:
            if logger:
                logger.error(f"Failed to decompress {file_path}: {str(e)}")
            return False

    def _download_wiki_based_data(
        self, 
        data_type: str, 
        wiki_codes: List[str]
    ) -> Dict[str, int]:
        """Download wiki-based data like revisions and pagelinks"""
        stats = {"attempted": 0, "success": 0}
        
        for wiki_code in wiki_codes:
            # Get available dumps
            url = self.BASE_URLS[data_type].format(wiki_code=wiki_code)
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                
                # Extract dump dates (YYYYMMDD format)
                dump_dates = re.findall(r'href="(\d{8})/"', response.text)
                latest_dump = sorted(dump_dates, reverse=True)[0] if dump_dates else None
                
                if not latest_dump:
                    self.logger.warning(f"No dumps found for {wiki_code}")
                    continue
                    
                # Get files for the latest dump
                dump_url = f"{url}{latest_dump}/"
                response = self.session.get(dump_url, timeout=10)
                response.raise_for_status()
                
                # Filter files based on data type
                if data_type == "revisions":
                    files = re.findall(r'href="(.*?-pages-meta-history.*?\.xml\.bz2)"', response.text)
                elif data_type == "pagelinks":
                    files = re.findall(r'href="(.*?-pagelinks\.sql\.gz)"', response.text)
                
                for filename in files:
                    stats["attempted"] += 1
                    if self._process_file(data_type, url, filename, dump_date=latest_dump, wiki_codes=[wiki_code]):
                        stats["success"] += 1
                        
            except requests.RequestException as e:
                self.logger.error(f"Error processing {wiki_code}: {str(e)}")
                
        return stats

    def _process_file(
        self,
        data_type: str,
        identifier: str,
        filename: str,
        year_month: Optional[str] = None,
        dump_date: Optional[str] = None,
        wiki_codes: Optional[List[str]] = None,
    ) -> bool:
        """Handle single file download and processing"""
        # Build appropriate URL based on data type
        if data_type in ["revisions", "pagelinks"]:
            file_url = f"{self.BASE_URLS[data_type].format(wiki_code=identifier)}{dump_date}/{filename}"
            self.logger.info(f"Downloading {filename} from {file_url}")
        else:
            file_url = f"{identifier}{filename}"
            self.logger.info(f"Downloading {filename} from {file_url}")

        # Determine output path and convert to Path object
        wiki_code = next((code for code in wiki_codes if code in filename), None) if wiki_codes else None
        output_path = Path(self._get_output_path(data_type, filename, year_month, dump_date, wiki_code))
        os.makedirs(output_path.parent, exist_ok=True)

        # Download file
        if not self.download_file(file_url, output_path):
            return False
        
        # Handle decompression if needed
        '''
        if output_path.suffix in (".gz", ".bz2"):
            return decompress_file(output_path, logger=self.logger)
        return True
        '''

    def _get_output_path(
        self, 
        data_type: str, 
        filename: str, 
        year_month: Optional[str] = None,
        dump_date: Optional[str] = None,
        wiki_code: Optional[str] = None
    ) -> Path:
        """Build appropriate output path based on data type and identifiers"""
        if data_type in ["pageviews", "clickstream"]:
            if year_month:
                year, month = year_month.split("-")
                return self.output_dir / data_type / year / month / filename
            return self.output_dir / data_type / filename
        
        elif data_type in ["revisions", "pagelinks"]:
            if wiki_code and dump_date:
                return self.output_dir / data_type / wiki_code / dump_date / filename
            
        # Default fallback
        return self.output_dir / data_type / filename

    def download_file(
        self,
        url: str,
        output_path: Path,
        max_retries: int = 3,
        chunk_size: int = 8192,
        timeout: int = 30,
        min_expected_bytes: Optional[int] = None,
    ) -> bool:
        """
        Robust file downloader with retries, progress tracking, size verification, and error handling.
        """
        if output_path.exists():
            self.logger.info(f"File already exists, skipping download: {output_path}")
            return True

        temp_path = output_path.with_suffix(".tmp")

        for attempt in range(max_retries):
            try:
                head = self.session.head(url, timeout=timeout, allow_redirects=True)
                head.raise_for_status()

                remote_size = int(head.headers.get("Content-Length", 0))
                if min_expected_bytes and remote_size < min_expected_bytes:
                    raise ValueError(
                        f"Remote file too small ({remote_size} < {min_expected_bytes} bytes)"
                    )

                if output_path.exists():
                    local_size = output_path.stat().st_size
                    if local_size == remote_size:
                        self.logger.info(f"File exists with correct size: {output_path}")
                        return True

                start_time = datetime.now()
                with self.session.get(url, stream=True, timeout=timeout) as r:
                    r.raise_for_status()
                    total = int(r.headers.get("Content-Length", 0))
                    with open(temp_path, "wb") as f, tqdm(
                        total=total, unit='B', unit_scale=True, unit_divisor=1024,
                        desc=f"Downloading {output_path.name}", initial=0
                    ) as bar:
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                bar.update(len(chunk))

                if remote_size > 0 and temp_path.stat().st_size != remote_size:
                    raise IOError(
                        f"Size mismatch: {temp_path.stat().st_size} != {remote_size}"
                    )

                temp_path.rename(output_path)
                self.logger.info(f"Successfully downloaded: {output_path}")
                return True

            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Download failed after {max_retries} attempts: {str(e)}")
                    if temp_path.exists():
                        temp_path.unlink()
                    return False

                wait_time = min(2**attempt, 60)
                self.logger.warning(f"Attempt {attempt + 1} failed. Retrying in {wait_time}s...")
                time.sleep(wait_time)

        return False


# Simple function interface for direct downloads
def download_file(url: str, target_path: Path, logger: Optional[logging.Logger] = None) -> bool:
    """
    Simple interface for one-off file downloads.
    Creates a temporary downloader instance and performs the download.
    """
    downloader = WikimediaDownloader(target_path.parent, logger=logger)
    return downloader.download_file(url, target_path)