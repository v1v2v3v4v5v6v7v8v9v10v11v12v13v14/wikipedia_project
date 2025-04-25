#!/usr/bin/env python3
import argparse
import logging
from pathlib import Path

from processing.orchestrator import WikipediaDataOrchestrator
from processing.parser import WikipediaStreamParser
from persistence.mongo_service import MongoPersistenceService
from wiki_utils.compression import decompress_file
from logging_utils import ApplicationLogger


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description='Process Wikipedia dump files')
    parser.add_argument('input_path', type=str, help='Path to input file or directory')
    parser.add_argument('--output-dir', type=str, default='./data', help='Output directory')
    parser.add_argument('--mongo-uri', type=str, default='mongodb://localhost:27017', 
                       help='MongoDB connection URI')
    parser.add_argument('--batch-size', type=int, default=100, help='Processing batch size')
    args = parser.parse_args()

    # Initialize
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger = ApplicationLogger()
    parser = WikipediaStreamParser()
    persistence = MongoPersistenceService(args.mongo_uri, logger=logger)
    
    orchestrator = WikipediaDataOrchestrator(
        stream_parser=parser,
        persistence_service=persistence,
        logger=logger,
        batch_size=args.batch_size,
        output_dir=output_dir
    )

    # Process input
    input_path = Path(args.input_path)
    if input_path.is_file():
        if input_path.suffix in ('.gz', '.bz2'):
            input_path = decompress_file(input_path, output_dir, delete_original=False)
        orchestrator.process_dump_file(input_path)
    elif input_path.is_dir():
        for file in input_path.glob('*'):
            if file.suffix in ('.gz', '.bz2'):
                file = decompress_file(file, output_dir, delete_original=False)
            orchestrator.process_dump_file(file)
    else:
        logger.error(f"Invalid input path: {input_path}")
        return

if __name__ == "__main__":
    main()