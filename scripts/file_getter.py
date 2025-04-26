"""
Main script to coordinate Wikimedia data downloading and discovery.
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
from wiki_utils.wiki_downloader import WikimediaDownloader
from wiki_io.file_discoverer import FileDiscoverer

class WikimediaDataManager:
    """
    Coordinates between file discovery and downloading functionality.
    """
    
    def __init__(
        self,
        output_dir: Path,
        log_dir: Optional[Path] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the data manager with output directories and logging.
        
        Args:
            output_dir: Base directory for downloaded files
            log_dir: Directory for log files (defaults to output_dir/logs)
            logger: Optional pre-configured logger
        """
        self.output_dir = output_dir
        self.log_dir = log_dir or output_dir / "logs"
        
        # Set up logging if not provided
        self.logger = logger or self._setup_logging()
        
        # Initialize components
        self.downloader = WikimediaDownloader(output_dir, logger=self.logger)
        self.discoverer = FileDiscoverer(output_dir, logger=self.logger)
    
    def _setup_logging(self) -> logging.Logger:
        """Configure basic logging for the application."""
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        logger = logging.getLogger("WikimediaDataManager")
        logger.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        
        # File handler
        log_file = self.log_dir / f"wikimedia_data_{datetime.now().date()}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def get_available_files(
        self,
        data_type: str,
        years: Optional[List[int]] = None,
        months: Optional[List[int]] = None,
        wiki_codes: Optional[List[str]] = None,
        local_only: bool = False,
        remote_only: bool = False
    ) -> List[str]:
        """
        Get list of available files (either locally, remotely, or both).
        
        Args:
            data_type: One of ['pageviews', 'clickstream', 'revisions', 'pagelinks']
            years: List of years for time-based data
            months: List of months for time-based data
            wiki_codes: List of wiki project codes
            local_only: Only return locally available files
            remote_only: Only return remotely available files
            
        Returns:
            List of file paths/names
        """
        if local_only:
            return self.discoverer.discover_local_files(
                data_type, years, months, wiki_codes
            )
        
        if remote_only:
            return self.discoverer.discover_all_files(
                data_type, years, months, wiki_codes
            )[len(self.discoverer.discover_local_files(data_type, years, months, wiki_codes)):]
        
        return self.discoverer.discover_all_files(
            data_type, years, months, wiki_codes
        )
    
    def download_files(
        self,
        data_type: str,
        years: Optional[List[int]] = None,
        months: Optional[List[int]] = None,
        wiki_codes: Optional[List[str]] = None
    ) -> Dict[str, int]:
        """
        Download files of the specified type, optionally filtered by time/wiki codes.
        
        Args:
            data_type: One of ['pageviews', 'clickstream', 'revisions', 'pagelinks']
            years: List of years for time-based data
            months: List of months for time-based data
            wiki_codes: List of wiki project codes
            
        Returns:
            Dictionary with download statistics
        """
        return self.downloader.download(
            data_type, years, months, wiki_codes
        )
    
    def sync_files(
        self,
        data_type: str,
        years: Optional[List[int]] = None,
        months: Optional[List[int]] = None,
        wiki_codes: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Synchronize files - download any missing files that aren't already local.
        
        Args:
            data_type: One of ['pageviews', 'clickstream', 'revisions', 'pagelinks']
            years: List of years for time-based data
            months: List of months for time-based data
            wiki_codes: List of wiki project codes
            
        Returns:
            Dictionary with sync statistics
        """
        stats = {
            "total_files": 0,
            "already_local": 0,
            "downloaded": 0,
            "failed": 0
        }
        
        # Get all remote files
        all_files = self.get_available_files(
            data_type, years, months, wiki_codes, remote_only=True
        )
        stats["total_files"] = len(all_files)
        
        if not all_files:
            self.logger.warning(f"No files found for {data_type} with given filters")
            return stats
        
        # Get local files to know what we already have
        local_files = self.get_available_files(
            data_type, years, months, wiki_codes, local_only=True
        )
        local_filenames = {Path(f).name for f in local_files}
        stats["already_local"] = len(local_files)
        
        # Download missing files
        for filename in all_files:
            if Path(filename).name in local_filenames:
                continue
                
            self.logger.info(f"Downloading missing file: {filename}")
            if data_type in ["pageviews", "clickstream"]:
                # Time-based data
                year = int(filename.split("-")[1][:4])
                month = int(filename.split("-")[1][4:6])
                result = self.download_files(
                    data_type, years=[year], months=[month], wiki_codes=wiki_codes
                )
            else:
                # Wiki-based data
                wiki_code = next(
                    (code for code in (wiki_codes or []) if code in filename), 
                    filename.split("-")[0]
                )
                result = self.download_files(
                    data_type, wiki_codes=[wiki_code]
                )
            
            if result["success"] > 0:
                stats["downloaded"] += 1
            else:
                stats["failed"] += 1
        
        return stats


if __name__ == "__main__":
    # Example usage
    output_dir = Path("./AAA/wikimedia").expanduser()
    manager = WikimediaDataManager(output_dir)
    
    # Example 1: Discover available files
    print("Available pageview files:")
    pageview_files = manager.get_available_files(
        "pageviews", 
        years=[2023], 
        months=[1, 2], 
        wiki_codes=["enwiki"]
    )
    print(pageview_files[:5])  # Print first 5 files
    
    # Example 2: Download specific files
    print("\nDownloading clickstream data:")
    download_stats = manager.download_files(
        "clickstream",
        years=[2023],
        months=[1],
        wiki_codes=["enwiki", "dewiki"]
    )
    print(f"Download stats: {download_stats}")
    """
    # Example 3: Sync files (download missing ones)
    print("\nSyncing revision files:")
    sync_stats = manager.sync_files(
        "revisions",
        wiki_codes=["enwiki", "frwiki"]
    )
    print(f"Sync stats: {sync_stats}")
    """