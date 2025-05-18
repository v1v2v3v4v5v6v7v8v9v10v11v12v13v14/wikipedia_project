from persistence.mongo_service import MongoPersistenceService

from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path
from typing import Dict, List

import concurrent
from downloading.wiki_downloader import WikimediaDownloader

class DownloadManager:
    def __init__(self, output_dir: Path, max_workers: int = 3, parsers: dict = None):
        self.output_dir = output_dir
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.logger = logging.getLogger(__name__)
        # Store parser mapping for use by download/process methods if needed
        self.parsers = parsers or {}
    def batch_download(self, requests: List[Dict]) -> Dict[str, Dict]:
        """
        Process multiple download requests concurrently
        Format: [
            {"data_type": "clickstream", "years": [2023], "months": [1,2]},
            {"data_type": "revisions", "wiki_codes": ["enwiki"]}
        ]
        """
        results = {}
        futures = {}
        
        for req in requests:
            data_type = req['data_type']
            downloader = WikimediaDownloader(
                self.output_dir / data_type,
                logger=self.logger,

            )
            future = self.executor.submit(
                self._run_download,
                downloader,
                req
            )
            futures[future] = data_type

        for future in concurrent.futures.as_completed(futures):
            data_type = futures[future]
            try:
                results[data_type] = future.result()
            except Exception as e:
                self.logger.error(f"Download failed for {data_type}: {str(e)}")
                results[data_type] = {"error": str(e)}

        return results

    def _run_download(self, downloader: WikimediaDownloader, params: Dict):
        """Thread-safe download execution"""
        return downloader.download(**params)
class MongoManager:
    """
    Encapsulates MongoDB operations using MongoPersistenceService.
    Provides clear methods for saving documents, revisions, pageviews, clickstreams, and pagelinks.
    """
    @staticmethod
    def make_composite_key(*args) -> str:
        """
        Join provided arguments with '|' to form a composite key.
        Example: make_composite_key('desktop', 'user', '2025-05') -> "desktop|user|2025-05"
        """
        return "|".join(str(arg) for arg in args)
    def __init__(self, mongo_uri: str, db_name: str, schema_path: str):
        self.logger = logging.getLogger(__name__)
        try:
            self.service = MongoPersistenceService(mongo_uri, db_name, schema_path)
            self.logger.info("MongoPersistenceService initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize MongoPersistenceService: {e}")
            raise

    def save_document(self, document: dict) -> bool:
        """Save a WikiDocument."""
        try:
            result = self.service.save_document(document)
            if result:
                self.logger.info(f"Document saved: {document.get('page_id', 'unknown')}")
            else:
                self.logger.warning(f"Failed to save document: {document.get('page_id', 'unknown')}")
            return result
        except Exception as e:
            self.logger.error(f"Exception in save_document: {e}")
            return False

    def save_revision(self, revision: dict) -> bool:
        """Save a RevisionData record."""
        try:
            result = self.service.save_revision(revision)
            if result:
                self.logger.info(f"Revision saved: {revision.get('rev_id', 'unknown')}")
            else:
                self.logger.warning(f"Failed to save revision: {revision.get('rev_id', 'unknown')}")
            return result
        except Exception as e:
            self.logger.error(f"Exception in save_revision: {e}")
            return False

    def save_pageview(self, pageview_data: dict) -> bool:
        """Save a PageView record."""
        try:
            # build composite key for indexing
            key = self.make_composite_key(
                pageview_data.get('access_type'),
                pageview_data.get('user_type'),
                pageview_data.get('year_month')
            )
            pageview_data['composite_key'] = key
            result = self.service.save_pageview(pageview_data)
            if result:
                self.logger.info(f"PageView saved for: {pageview_data.get('page_hash', 'unknown')}")
            else:
                self.logger.warning(f"Failed to save pageview: {pageview_data.get('page_hash', 'unknown')}")
            return result
        except Exception as e:
            self.logger.error(f"Exception in save_pageview: {e}")
            return False

    def save_clickstream(self, clickstream_data: dict) -> bool:
        """Save a ClickStream record."""
        try:
            # build composite key for indexing
            key = self.make_composite_key(
                clickstream_data.get('year_month'),
                clickstream_data.get('target_page')
            )
            clickstream_data['composite_key'] = key
            result = self.service.save_clickstream(clickstream_data)
            if result:
                self.logger.info(f"Clickstream saved: {clickstream_data.get('target_page', 'unknown')}")
            else:
                self.logger.warning(f"Failed to save clickstream: {clickstream_data.get('target_page', 'unknown')}")
            return result
        except Exception as e:
            self.logger.error(f"Exception in save_clickstream: {e}")
            return False

    def save_pagelink(self, pagelink_data: dict) -> bool:
        """Save a PageLinks record."""
        try:
            result = self.service.save_pagelink(pagelink_data)
            if result:
                self.logger.info(f"Pagelink saved: {pagelink_data.get('pl_from', 'unknown')}")
            else:
                self.logger.warning(f"Failed to save pagelink: {pagelink_data.get('pl_from', 'unknown')}")
            return result
        except Exception as e:
            self.logger.error(f"Exception in save_pagelink: {e}")
            return False

    def close(self):
        """Close the MongoDB connection."""
        try:
            self.service.close()
            self.logger.info("MongoManager closed MongoDB connection.")
        except Exception as e:
            self.logger.error(f"Exception during MongoManager close: {e}")
