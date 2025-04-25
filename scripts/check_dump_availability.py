#!/usr/bin/env python3
import argparse
import json
import logging
from pathlib import Path
from typing import List

from wiki_utils.wikimedia_checker import WikimediaDumpChecker
from wiki_utils.downloader import download_file

def configure_logging(verbose: bool) -> logging.Logger:
    """Set up console logging"""
    logger = logging.getLogger('dump_checker')
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logger

def main():
    parser = argparse.ArgumentParser(description='Check Wikimedia dump availability')
    parser.add_argument(
        'years',
        type=int,
        nargs='+',
        help='Years to check (e.g., 2023 2024)'
    )
    parser.add_argument(
        '--months',
        type=int,
        nargs='+',
        default=list(range(1, 13)),
        help='Months to check (1-12)'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('./dump_reports'),
        help='Directory for reports and downloads'
    )
    parser.add_argument(
        '--download',
        action='store_true',
        help='Download available files after checking'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output'
    )
    args = parser.parse_args()

    # Setup environment
    logger = configure_logging(args.verbose)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Check availability
    checker = WikimediaDumpChecker()
    results = checker.check_pageview_availability(args.years, args.months)
    
    # Save report
    report_file = args.output_dir / 'availability.json'
    checker.save_availability_report(results, report_file)
    logger.info(f"Saved availability report to {report_file}")

    # Optional download
    if args.download:
        download_plan = checker.generate_download_plan(results, args.output_dir)
        logger.info(f"Found {len(download_plan)} files to download")
        
        for url, target_path in download_plan:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Downloading {url} to {target_path}")
            download_file(url, target_path, logger=logger)

if __name__ == "__main__":
    main()