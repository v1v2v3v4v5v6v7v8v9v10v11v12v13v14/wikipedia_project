#!/usr/bin/env python3
import argparse
import sys
from typing import List, Optional, Dict
from pathlib import Path
import logging
import json
from icecream import ic

# Import WikimediaDownloader components
from downloading.wiki_downloader import WikimediaDownloader
# Import DownloadManager, and parser classes for explicit linking
from management.manager import DownloadManager
# Import parser components
from processing.parser.tsv_parser import TSVParser
from processing.parser.xml_parser import RevisionParser
from persistence.mongo_service import MongoPersistenceService

class CLISystem:
    def __init__(self):
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("cli_system.log")
            ]
        )
        self.logger = logging.getLogger("CLISystem")
        self.parser = self._setup_parser()
        
    def _setup_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Wikipedia Data Processing CLI System",
            formatter_class=argparse.RawTextHelpFormatter
        )
        
        # Command argument
        parser.add_argument('command', 
                           choices=['download', 'process', 'status', 'clean', 'help'],
                           help='Command to execute')
        
        # Download command arguments
        parser.add_argument('data_type', nargs='?',
                           choices=['pageviews', 'clickstream', 'revisions', 'pagelinks'],
                           help='Type of data to download (for download command)')
        
        # Download options
        parser.add_argument('-y', '--years', type=int, nargs='+', 
                           help='Years to download (for pageviews/clickstream)')
        parser.add_argument('-m', '--months', type=int, nargs='+', 
                           help='Months to download (for pageviews/clickstream)')
        parser.add_argument('-w', '--wiki-codes', nargs='+', 
                           help='Wiki codes to download (e.g., enwiki)')
        parser.add_argument('-o', '--output', 
                           help='Output directory (required for download command)', default='./output/downloads')
        parser.add_argument('-j', '--parallel', type=int, default=3, 
                           help='Number of parallel downloads')
        parser.add_argument('-b', '--batch', type=str, 
                           help='Batch file with download specifications (JSON)')
        
        # Process command arguments
        parser.add_argument('-i', '--input', 
                           help='Input directory (required for process command)')
        parser.add_argument('-c', '--config', 
                           help='Configuration file')
        
        # Status command arguments
        parser.add_argument('-d', '--detailed', action='store_true', 
                           help='Detailed output (for status command)')
        
        # Clean command arguments
        parser.add_argument('target', nargs='?',
                           choices=['cache', 'temp', 'all'], 
                           help='Target to clean (for clean command)')
        parser.add_argument('-f', '--force', action='store_true', 
                           help='Skip confirmation (for clean command)')
        
        return parser

    def run(self):
        args = self.parser.parse_args()
        
        if args.command == 'download':
            self._handle_download(args)
        elif args.command == 'process':
            self._handle_process(args)
        elif args.command == 'status':
            self._handle_status(args)
        elif args.command == 'clean':
            self._handle_clean(args)
        elif args.command == 'help':
            self._handle_help(args)
        else:
            self.parser.print_help()

    # --- Download Command - Modified for Wikimedia Data ---
    def _handle_download(self, args):
        if not self._validate_download_args(args):
            sys.exit(1)
            
        if args.batch:
            self._execute_batch_downloader(args.batch, args.output, args.parallel)
        else:
            self._execute_downloader(
                data_type=args.data_type, 
                years=args.years, 
                months=args.months, 
                wiki_codes=args.wiki_codes,
                output_dir=args.output,
                parallel=args.parallel
            )

    def _validate_download_args(self, args) -> bool:
        """Validate download arguments"""
        # Ensure we have output directory for all download operations
        if not args.output:
            self.logger.error("Output directory (-o/--output) is required for download command")
            return False
            
        # Batch mode validation
        if args.batch:
            if not Path(args.batch).exists():
                self.logger.error(f"Batch file not found: {args.batch}")
                return False
            try:
                with open(args.batch, 'r') as f:
                    batch_data = json.load(f)
                # Validate batch data structure here if needed
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON in batch file: {e}")
                return False
            return True
            
        # Direct download mode requires data_type
        if not args.data_type:
            self.logger.error("Data type is required for download command")
            return False
            
        # Direct mode validation
        if args.data_type in ['pageviews', 'clickstream']:
            if not args.years and not args.months:
                self.logger.warning("No years/months specified, will use defaults")
                
        if args.data_type in ['revisions', 'pagelinks']:
            if not args.wiki_codes:
                self.logger.error(f"Wiki codes are required for {args.data_type}")
                return False
                
        output_path = Path(args.output)
        if not output_path.exists():
            try:
                output_path.mkdir(parents=True)
                self.logger.info(f"Created output directory: {output_path}")
            except OSError as e:
                self.logger.error(f"Cannot create output directory: {e}")
                return False
                
        return True

    def _execute_batch_downloader(self, batch_file: str, output_dir: str, parallel: int):
        """Execute batch downloads from JSON specification

        Integration flow:
        CLI (CLISystem)
            -> DownloadManager (management/manager.py)
                -> Parsers (TSVParser, RevisionParser, etc.)
        Parsers are explicitly linked and passed to the DownloadManager here.
        Handles multiple wiki codes by creating parser instances per code.
        """
        self.logger.info(f"Running batch download from {batch_file}")
        try:
            with open(batch_file, 'r') as f:
                batch_requests = json.load(f)

            # Explicit parser linkage with support for multiple wiki codes:
            # If a batch request contains multiple wiki_codes, we create a separate parser instance for each code.
            # This ensures that the parser_map can differentiate by both data_type and wiki_code.
            parser_map = {}
            for req in batch_requests:
                data_type = req.get("data_type")
                wiki_codes = ic(req.get("wiki_codes")) or []
                # If no wiki_codes specified, use None as key
                if not wiki_codes:
                    if data_type in ["pageviews", "clickstream"]:
                        parser_map[(data_type, None)] = TSVParser(logger=self.logger)
                    elif data_type in ["revisions", "pagelinks"]:
                        parser_map[(data_type, None)] = RevisionParser(logger=self.logger)
                    continue
                for wiki_code in wiki_codes:
                    # Create a dedicated parser instance per (data_type, wiki_code)
                    if data_type in ["pageviews", "clickstream"]:
                        parser_map[(data_type, wiki_code)] = TSVParser(logger=self.logger)
                    elif data_type in ["revisions", "pagelinks"]:
                        parser_map[(data_type, wiki_code)] = RevisionParser(logger=self.logger)
                        
            # Documented: parser_map now supports (data_type, wiki_code) keys for explicit multi-wiki handling
            download_manager = DownloadManager(
                output_dir=Path(output_dir),
                max_workers=parallel,
                parsers=parser_map
            )
            # Execute batch download
            results = download_manager.batch_download(batch_requests)
            # Report results
            for data_type, result in results.items():
                if "error" in result:
                    self.logger.error(f"Download failed for {data_type}: {result['error']}")
                else:
                    self.logger.info(
                        f"Download results for {data_type}: "
                        f"Attempted: {result.get('attempted', 0)}, "
                        f"Success: {result.get('success', 0)}"
                    )
        except Exception as e:
            self.logger.error(f"Batch download failed: {e}")

    def _execute_downloader(
        self, 
        data_type: str, 
        output_dir: str,
        years: Optional[List[int]] = None,
        months: Optional[List[int]] = None,
        wiki_codes: Optional[List[str]] = None,
        parallel: int = 3
    ):
        """Execute direct download with WikimediaDownloader"""
        self.logger.info(f"Downloading {data_type} data to {output_dir}")
        
        try:
            # Create downloader instance
            downloader = WikimediaDownloader(
                output_dir=Path(output_dir),
                logger=self.logger
            )
            
            # Execute download
            results = downloader.download(
                data_type=data_type,
                years=years,
                months=months,
                wiki_codes=wiki_codes
            )
            
            # Report results
            self.logger.info(
                f"Download results: "
                f"Attempted: {results.get('attempted', 0)}, "
                f"Success: {results.get('success', 0)}"
            )
            
        except Exception as e:
            self.logger.error(f"Download failed: {e}")

    # --- Process Command ---
    def _handle_process(self, args):
        if not self._validate_process_args(args):
            sys.exit(1)
        download_requests = [{
            "data_type": args.data_type,
            "years": args.years,
            "months": args.months,
            "wiki_codes": args.wiki_codes
        }]

        # Explicitly link parser classes here for DownloadManager
        # Integration flow: CLI -> DownloadManager -> Parsers
        parser_map = {}
        # Loop through wiki_codes and initialize parser instances with wiki_code
        wiki_codes = args.wiki_codes if args.wiki_codes else [None]
        for wiki_code in wiki_codes:
            if args.data_type in ["pageviews", "clickstream"]:
                parser_map[(args.data_type, wiki_code)] = TSVParser(logger=self.logger, wiki_code=wiki_code)
            elif args.data_type in ["revisions", "pagelinks"]:
                parser_map[(args.data_type, wiki_code)] = RevisionParser(logger=self.logger, wiki_code=wiki_code)
        download_manager = DownloadManager(
            output_dir=Path(args.output),
            max_workers=args.parallel,
            parsers=parser_map
        )
        download_results = download_manager.batch_download(download_requests)

        files_processed = 0
        for data_type, result in download_results.items():
            if "error" in result:
                self.logger.error(f"Download error for {data_type}: {result['error']}")
                continue

            # Select parser based on data_type explicitly
            parser = parser_map.get((data_type, wiki_code))
            if not parser:
                ic(parser_map, data_type)
                self.logger.error(f"No parser available for data type: {data_type}")
                continue

            # Prepare wiki_filter for parser
            wiki_codes = set(args.wiki_codes) if args.wiki_codes else None

            # Process each downloaded file
            download_path = download_manager.output_dir / data_type
            # Initialize MongoDB persistence service
            mongo_service = MongoPersistenceService(
                mongo_uri="mongodb://localhost:27017",  # <-- Update this URI as needed
                db_name="wikipedia_data",               # <-- Update this database name as needed
                schema_path="/Applications/wikipedia_refactor_target/avro_utils/avro_schemas.json"       # <-- Update this schema path as needed
            )
            for wiki_code in wiki_codes:
                for file_path in download_path.rglob('*'):
                    if file_path.is_file():
                        with file_path.open('rb') as stream:
                            for record in ic(parser.parse_stream(stream, file_path.name, wiki_filter=wiki_code)):
                                # Add filtering logic here if needed

                                # --- MongoDB Persistence Integration ---
                                # Persist the record to MongoDB using the appropriate method based on data_type
                                if data_type in ["clickstream"]:
                                    # Save as a document in the corresponding collection
                                    mongo_service.save_clickstream(record)
                                    # Documented: Each record is saved to the data_type-named collection
                                elif data_type in ["revisions", "pagelinks"]:
                                    # Save as a revision or pagelink document
                                    if data_type == "revisions":
                                        mongo_service.save_revision(record)
                                        # Documented: Each revision record is persisted using save_revision
                                    elif data_type == "pagelinks":
                                        mongo_service.save_pagelink(record)
                                        # Documented: Each pagelink record is persisted using save_pagelink
                                else:
                                    # Unknown data_type, optionally log or skip
                                    self.logger.warning(f"Unknown data_type for MongoDB persistence: {data_type}")

                                files_processed += 1

            self.logger.info(f"Integrated download-parse completed. Files processed: {files_processed}")

    def _validate_process_args(self, args) -> bool:
        """Validate process arguments for integrated download and parsing workflow"""
        # No input validation required for integrated workflow
        return True

    def _start_pipeline(self, input_dir: str, config_file: Optional[str]):
        """Simple processing example"""
        self.logger.info(f"Processing files in {input_dir}")
        if config_file:
            self.logger.info(f"Using config: {config_file}")
        # Add your actual processing logic here

    # --- Status Command ---
    def _handle_status(self, args):
        status = self._check_system_health()
        self._generate_report(status, args.detailed)

    def _check_system_health(self) -> Dict:
        """Collect system metrics"""
        # Import psutil here to make it optional
        import psutil
        return {
            'cpu': psutil.cpu_percent(),
            'memory': psutil.virtual_memory().percent,
            'disk': psutil.disk_usage('/').percent
        }

    def _generate_report(self, status: Dict, detailed: bool):
        """Print formatted status report"""
        self.logger.info("\n=== SYSTEM STATUS ===")
        self.logger.info(f"CPU Usage: {status['cpu']}%")
        self.logger.info(f"Memory Usage: {status['memory']}%")
        self.logger.info(f"Disk Usage: {status['disk']}%")
        
        if detailed:
            import psutil
            self.logger.info("\nDetailed Info:")
            self.logger.info(f"Processes: {len(psutil.pids())}")
            self.logger.info(f"Boot Time: {psutil.boot_time()}")

    # --- Clean Command ---
    def _handle_clean(self, args):
        if not self._validate_clean_args(args):
            sys.exit(1)
            
        if not args.force and not self._confirm_action(args.target):
            self.logger.info("Cleanup aborted")
            return
            
        self._execute_cleanup(args.target)

    def _validate_clean_args(self, args) -> bool:
        """Validate clean arguments"""
        if not args.target:
            self.logger.error("Target is required for clean command")
            return False
            
        # Additional validation logic here
        return True

    def _confirm_action(self, target: str) -> bool:
        """Prompt for user confirmation"""
        resp = input(f"Confirm clean {target}? [y/N] ").lower()
        return resp == 'y'

    def _execute_cleanup(self, target: str):
        """Perform cleanup operations"""
        self.logger.info(f"Cleaning {target}...")
        # Add actual cleanup logic here
        self.logger.info("Cleanup complete")

    # --- Help Command ---
    def _handle_help(self, args):
        self.parser.print_help()

if __name__ == "__main__":
    try:
        cli = CLISystem()
        cli.run()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)