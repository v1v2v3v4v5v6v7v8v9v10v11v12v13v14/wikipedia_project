"""Parser for Wikimedia XML revision data from compressed/plain files."""

import logging
import hashlib
import os
import re
import bz2
import gzip
from datetime import datetime, timezone
from typing import Iterator, Optional, Dict, Any, TextIO, List, Union
from lxml import etree
from icecream import ic

from processing.parser.base_parser import BaseParser
from wiki_utils.datetime_utils import parse_wikimedia_timestamp

# XML Namespace for Wikimedia dumps
WIKI_NS = {'mw': 'http://www.mediawiki.org/xml/export-0.11/'}

class RevisionParser(BaseParser):
    """Streaming parser for Wikimedia XML revision data."""
    
    def __init__(self, wiki_code: str, logger: Optional[logging.Logger] = None):
        """
        Initialize parser.
        
        Args:
            wiki_code: Wiki identifier (e.g., 'enwiki')
            logger: Optional custom logger
        """
        super().__init__(logger)
        self.wiki_code = wiki_code
        self._stats = {
            'pages': 0,
            'revisions': 0,
            'skipped_pages': 0,
            'skipped_revisions': 0
        }
        self._last_updated = None

    def parse_stream(
        self,
        stream_or_path: Any,
        file_name: str,
        sample_limit: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Parse a single file stream.
        
        Args:
            stream_or_path: File-like object or path to file
            file_name: Name of the file being parsed
            sample_limit: Optional maximum number of records to yield
            
        Returns:
            Iterator of dictionaries containing parsed records
        """
        # Get appropriate binary stream based on compression
        binary_stream = self._get_binary_stream(stream_or_path, file_name)
        
        try:
            self._last_updated = None
            
            # Extract metadata from filename
            file_wiki, file_date = self._extract_metadata_from_filename(file_name)
            
            # Use lxml's iterparse for memory-efficient parsing
            context = etree.iterparse(
                binary_stream, 
                events=('end',), 
                tag=f'{{{WIKI_NS["mw"]}}}page'
            )

            for _, page_elem in context:
                self._stats['pages'] += 1
                
                # Extract page data
                page_data = ic(self._extract_page_data(page_elem))
                
                # Skip if no data or should be skipped
                if not page_data or page_data['should_skip']:
                    self._stats['skipped_pages'] += 1
                    page_elem.clear()
                    continue
                
                # Attach file-level metadata
                page_data['wiki_code'] = file_wiki or self.wiki_code
                page_data['file_date'] = file_date.isoformat() if file_date else None

                # Process all revisions for this page
                for revision in self._process_page_revisions(page_elem, page_data):
                    yield revision
                    
                    # Update last timestamp if needed
                    if 'timestamp' in revision and revision['timestamp']:
                        if not self._last_updated or revision['timestamp'] > self._last_updated:
                            self._last_updated = revision['timestamp']
                    
                    # Check sample limit
                    if sample_limit and self._stats['revisions'] >= sample_limit:
                        return

                # Clear element to free memory
                page_elem.clear()
                
        finally:
            # Clean up
            try:
                binary_stream.close()
            except Exception as e:
                self.logger.warning(f"Error closing stream: {e}")

    def _get_binary_stream(self, stream_or_path: Any, file_name: str) -> Any:
        """
        Get binary stream from file object or path.
        
        Args:
            stream_or_path: File-like object or path to file
            file_name: Name of the file for determining compression
            
        Returns:
            Binary stream
        """
        # Determine if we need to open a file
        should_close = False
        if not hasattr(stream_or_path, "read"):
            file_obj = open(stream_or_path, "rb")
            should_close = True
        else:
            file_obj = stream_or_path
            
        # Handle compression based on file extension
        try:
            if file_name.endswith(".bz2"):
                return bz2.BZ2File(file_obj, "rb")
            elif file_name.endswith(".gz"):
                return gzip.GzipFile(fileobj=file_obj, mode="rb")
            else:
                return file_obj
        except Exception as e:
            # Clean up if we opened the file
            if should_close:
                try:
                    file_obj.close()
                except Exception:
                    pass
            raise e

    def _extract_metadata_from_filename(self, file_name: str) -> tuple:
        """
        Extract wiki code and date from filename.
        
        Args:
            file_name: Name of file
            
        Returns:
            Tuple of (wiki_code, date)
        """
        file_base = os.path.basename(file_name)
        match = re.match(r'([a-z]+)-(\d{8})', file_base)
        if match:
            file_wiki, date_str = match.groups()
            try:
                file_date = datetime.strptime(date_str, "%Y%m%d").date()
                return file_wiki, file_date
            except ValueError:
                pass
        return self.wiki_code, None

    def _extract_page_data(self, page_elem: etree._Element) -> Optional[Dict[str, Any]]:
        """
        Extract metadata from page element.
        
        Args:
            page_elem: lxml Element for page
            
        Returns:
            Dictionary with page data or None if invalid
        """
        try:
            page_id = page_elem.findtext('./mw:id', namespaces=WIKI_NS) or ""
            title = page_elem.findtext('./mw:title', namespaces=WIKI_NS)
            ns = int(page_elem.findtext('./mw:ns', default='0', namespaces=WIKI_NS))
            is_redirect = page_elem.find('./mw:redirect', namespaces=WIKI_NS) is not None
            
            return {
                'page_id': page_id,
                'title': title,
                'namespace': ns,
                'is_redirect': is_redirect,
                'hash_id': hashlib.sha256(f"{self.wiki_code}-{page_id}".encode()).hexdigest(),
                'should_skip': is_redirect or ns != 0  # Skip redirects and non-main namespace
            }
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Invalid page data: {e}")
            return None

    def _process_page_revisions(self, page_elem: etree._Element, page_data: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Process all revisions for a single page.
        
        Args:
            page_elem: lxml Element for page
            page_data: Dictionary with page metadata
            
        Returns:
            Iterator of revision dictionaries
        """
        for rev_elem in page_elem.findall('./mw:revision', namespaces=WIKI_NS):
            revision_data = self._extract_revision_data(rev_elem)
            
            if revision_data:
                # Attach page-level metadata
                revision_data.update({
                    'page_id': page_data['page_id'],
                    'title': page_data['title'],
                    'wiki_code': page_data['wiki_code'],
                    'file_date': page_data['file_date'],
                    'namespace': page_data['namespace'],
                    'hash_id': page_data['hash_id']
                })

                yield revision_data
                self._stats['revisions'] += 1
            else:
                self._stats['skipped_revisions'] += 1

    def _extract_revision_data(self, rev_elem: etree._Element) -> Optional[Dict[str, Any]]:
        """
        Extract data from single revision element.
        
        Args:
            rev_elem: lxml Element for revision
            
        Returns:
            Dictionary with revision data or None if invalid/skipped
        """
        # Skip minor revisions
        if rev_elem.find('./mw:minor', namespaces=WIKI_NS) is not None:
            return None
            
        try:
            timestamp = parse_wikimedia_timestamp(
                rev_elem.findtext('./mw:timestamp', namespaces=WIKI_NS)
            )
            if not timestamp:
                return None
                
            contributor_data = self._extract_contributor(rev_elem)
            
            return {
                'rev_id': rev_elem.findtext('./mw:id', namespaces=WIKI_NS) or "",
                'timestamp': timestamp,
                'contributor': contributor_data.get('id', 0)
            }
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Invalid revision data: {e}")
            return None

    def _extract_contributor(self, rev_elem: etree._Element) -> Dict[str, Any]:
        """
        Extract contributor information.
        
        Args:
            rev_elem: lxml Element for revision
            
        Returns:
            Dictionary with contributor data
        """
        contrib = rev_elem.find('./mw:contributor', namespaces=WIKI_NS)
        
        if not contrib or contrib.get('deleted') == 'deleted':
            return {'id': 0}
            
        return {
            'id': int(contrib.findtext('./mw:id', default='0', namespaces=WIKI_NS)),
            'ip': contrib.findtext('./mw:ip', namespaces=WIKI_NS),
            'username': contrib.findtext('./mw:username', namespaces=WIKI_NS)
        }

    @property
    def stats(self) -> Dict[str, int]:
        """Get current processing statistics."""
        return self._stats.copy()
    
    @property
    def last_updated(self) -> Optional[datetime]:
        """Latest revision timestamp seen in the file."""
        return self._last_updated