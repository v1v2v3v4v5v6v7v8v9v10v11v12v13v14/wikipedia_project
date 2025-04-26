import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Union

@dataclass
class ProcessingStats:
    documents_processed: int = 0
    batches_processed: int = 0
    errors: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    file_metadata: Dict = field(default_factory=dict)

    def duration(self) -> float:
        return (self.end_time or time.time()) - self.start_time

class WikipediaDataOrchestrator:
    """
    Enhanced orchestrator with:
    - Better progress tracking
    - Resource management
    - Parallel processing options
    """

    def __init__(self, components: dict, config: dict):
        self.parser = components['parser']
        self.persistence = components['persistence']
        self.logger = components.get('logger', logging.getLogger(__name__))
        self.error_handler = components.get('error_handler')
        self.fetcher = components['fetcher']
        
        self.batch_size = config.get('batch_size', 100)
        self.output_dir = Path(config.get('output_dir', './data'))
        self.max_workers = config.get('max_workers', 1)
        
        self._validate_components()

    def _validate_components(self):
        """Validate required components and interfaces"""
        if not hasattr(self.fetcher, 'fetch_all'):
            raise ValueError("Fetcher must implement fetch_all()")
        if not hasattr(self.parser, 'parse_stream'):
            raise ValueError("Parser must implement parse_stream()")
        if not hasattr(self.persistence, 'bulk_save_documents'):
            raise ValueError("Persistence must implement bulk_save_documents()")

    def process_dump_file(self, file_path: Path) -> ProcessingStats:
        """Process single dump file with enhanced error handling"""
        stats = ProcessingStats(file_metadata={
            'filename': file_path.name,
            'size': file_path.stat().st_size
        })

        try:
            with self._open_file(file_path) as file:
                for batch in self._batch_generator(file):
                    self._process_batch(batch, stats)
                    
        except Exception as e:
            self._handle_processing_error(e, stats, file_path)
            
        stats.end_time = time.time()
        self._log_completion(stats)
        return stats

    def _open_file(self, file_path: Path):
        """Handle different file types (compressed/regular)"""
        if file_path.suffix == '.bz2':
            import bz2
            return bz2.open(file_path, 'rt')
        return file_path.open('r')

    def _batch_generator(self, file) -> Generator[List[Dict], None, None]:
        """Generate batches of parsed documents"""
        current_batch = []
        for doc in self.parser.parse_stream(file):
            current_batch.append(doc)
            if len(current_batch) >= self.batch_size:
                yield current_batch
                current_batch = []
        if current_batch:
            yield current_batch

    def _process_batch(self, batch: List[Dict], stats: ProcessingStats) -> None:
        """Process a single batch with error isolation"""
        try:
            result = self.persistence.bulk_save_documents(batch)
            stats.batches_processed += 1
            stats.documents_processed += len(batch)
            stats.errors += result.get('errors', 0)
            
            if stats.batches_processed % 10 == 0:  # Periodic progress
                self.logger.info(
                    f"Progress: {stats.documents_processed} docs "
                    f"({stats.errors} errors)"
                )
                
        except Exception as e:
            batch_error = {
                'batch_size': len(batch),
                'first_doc_id': batch[0].get('doc_id', 'unknown') if batch else None
            }
            if self.error_handler:
                self.error_handler.handle(e, batch_error)
            stats.errors += len(batch)

    def _handle_processing_error(self, error: Exception, stats: ProcessingStats, file_path: Path):
        """Centralized error handling"""
        if self.error_handler:
            self.error_handler.handle(error, {
                'context': 'file_processing',
                'file': str(file_path),
                'stats': stats.__dict__
            })
        stats.errors += 1  # Count the file-level error

    def _log_completion(self, stats: ProcessingStats) -> None:
        """Detailed completion logging"""
        self.logger.info(
            f"Completed processing: {stats.file_metadata['filename']}\n"
            f"• Documents: {stats.documents_processed}\n"
            f"• Batches: {stats.batches_processed}\n"
            f"• Errors: {stats.errors}\n"
            f"• Duration: {stats.duration():.2f}s\n"
            f"• Throughput: {stats.documents_processed/stats.duration():.1f} docs/sec"
        )

    def run(self):
        """End-to-end orchestrator: fetch files then process."""
        files = self.fetcher.fetch_all()
        for file_path in files:
            self.process_dump_file(file_path)

if __name__ == "__main__":
    import logging
    from pathlib import Path

    # Concrete component implementations
    from wiki_utils.file_getter import FileGetter           # must implement fetch_all()
    from wiki_utils.parser import WikipediaParser           # must implement parse_stream()
    from wiki_utils.persistence import MongoPersistence     # must implement bulk_save_documents()
    from wiki_utils.error_handler import ErrorHandler       # must implement handle()

    # Instantiate each component with hard-coded args for now
    fetcher = FileGetter(
        base_dirs={
            'pageviews': 'https://dumps.wikimedia.org/other/pageview_complete/',
            'clickstream': 'https://dumps.wikimedia.org/other/clickstream/'
        },
        years=[2023], months=[1],
        wiki_codes=['enwiki']
    )
    parser = WikipediaParser(schema_path="schemas/pageview.avsc", filters=None)
    persistence = MongoPersistence(
        uri="mongodb://localhost:27017",
        db_name="wikimedia",
        collection_name="pageviews"
    )
    error_handler = ErrorHandler(reporting_endpoint="https://errors.myapp.com")

    # Bundle components and config
    components = {
        'fetcher':       fetcher,
        'parser':        parser,
        'persistence':   persistence,
        'logger':        logging.getLogger("wikiorch"),
        'error_handler': error_handler
    }
    config = {
        'batch_size':   500,
        'output_dir':   "./data",
        'max_workers':  4
    }

    # Create and run orchestrator
    orchestrator = WikipediaDataOrchestrator(components, config)
    orchestrator.run()