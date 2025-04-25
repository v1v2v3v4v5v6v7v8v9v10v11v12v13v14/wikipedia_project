"""
Utilities for checking and downloading Wikimedia dump availability
"""

import re
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

class WikimediaDumpChecker:
    BASE_URL = "https://dumps.wikimedia.org/other/pageview_complete/"
    
    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.session.headers.update({'User-Agent': 'WikimediaDumpChecker/1.0'})

    def check_pageview_availability(
        self,
        years: List[int],
        months: range = range(1, 13),
        file_types: List[str] = ["pageviews", "geo"]
    ) -> Dict[str, Dict[str, List[str]]]:
        """
        Check available pageview dump files by year/month/type
        
        Returns nested dictionary:
        {
            "2023": {
                "01": ["pageviews-20230101-000000.gz", "geo-20230101-000000.gz"],
                ...
            },
            ...
        }
        """
        results = {}
        
        for year in years:
            year_str = str(year)
            results[year_str] = {}
            
            for month in months:
                month_str = f"{month:02d}"
                url = f"{self.BASE_URL}{year_str}/{year_str}-{month_str}/"
                
                try:
                    files = self._get_available_files(url, file_types)
                    if files:
                        results[year_str][month_str] = files
                except requests.RequestException as e:
                    continue
                    
        return results

    def _get_available_files(self, url: str, file_types: List[str]) -> List[str]:
        """Scrape index page for matching files"""
        response = self.session.get(url, timeout=10)
        response.raise_for_status()
        
        files = []
        for file_type in file_types:
            pattern = rf'href="({file_type}-\d{{8}}-\d{{6}}\.gz)"'
            files.extend(re.findall(pattern, response.text))
            
        return sorted(files)

    def generate_download_plan(
        self,
        availability: Dict[str, Dict[str, List[str]]],
        output_dir: Path
    ) -> List[Tuple[str, Path]]:
        """
        Generate download plan from availability results
        
        Returns list of (source_url, target_path) tuples
        """
        download_plan = []
        
        for year, months in availability.items():
            for month, files in months.items():
                for filename in files:
                    source_url = f"{self.BASE_URL}{year}/{year}-{month}/{filename}"
                    target_path = output_dir / year / month / filename
                    download_plan.append((source_url, target_path))
                    
        return download_plan

    @staticmethod
    def save_availability_report(
        results: Dict[str, Dict[str, List[str]]],
        output_file: Path
    ) -> None:
        """Save availability results to JSON file"""
        import json
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)