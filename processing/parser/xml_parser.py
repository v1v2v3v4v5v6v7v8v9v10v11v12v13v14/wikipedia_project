from datetime import datetime, timezone
import logging
import hashlib
import re
from typing import Iterator, Optional, Dict, Any, TextIO, List, Union
from lxml import etree
from icecream import ic

from processing.parser.base_parser import BaseParser
from wiki_utils.datetime_utils import (
    parse_wikimedia_timestamp,
    get_year_month_from_filename,
)
ic.enable()

WIKI_NS = {'mw': 'http://www.mediawiki.org/xml/export-0.11/'}

class RevisionParser(BaseParser):
    """Streaming parser for Wikimedia XML revision data."""
    
    def __init__(self, wiki_code: str, logger: Optional[logging.Logger] = None):
        super().__init__(logger)
        self.wiki_code = wiki_code
        self.stats = {
            'pages': 0,
            'revisions': 0,
            'skipped_pages': 0,
            'skipped_revisions': 0,
            'processed': 0,
            'errors': 0
        }
        self._last_updated = None

    def parse_stream(
        self,
        stream: TextIO,
        file_name: str,
        sample_limit: Optional[int] = None,
        batch_size: Optional[int] = None
    ) -> Union[Iterator[Dict[str, Any]], Iterator[List[Dict[str, Any]]]]:
        """Parse XML stream yielding either individual revisions or batches.
        
        Args:
            stream: Input XML stream
            file_name: Source filename for metadata
            sample_limit: Max revisions to process
            batch_size: If None yields single revisions, else yields batches of this size
            
        Returns:
            Iterator of revision dicts or batches of revision dicts
        """
        self._last_updated = None
        revisions_yielded = 0
        current_batch = []
        
        try:
            file_wiki, file_date = self._extract_metadata_from_filename(file_name)
            context = etree.iterparse(
                stream, 
                events=('end',), 
                tag=f'{{{WIKI_NS["mw"]}}}page'
            )

            for _, page_elem in context:
                self.stats['pages'] += 1
                page_data = self._extract_page_data(page_elem)
                
                if not page_data or page_data['should_skip']:
                    self.stats['skipped_pages'] += 1
                    page_elem.clear()
                    continue
                
                page_data.update({
                    'wiki_code': file_wiki or self.wiki_code,
                    'file_date': file_date.isoformat() if file_date else None
                })

                for revision in self._process_page_revisions(page_elem, page_data):
                    if batch_size is None:
                        yield revision
                    else:
                        current_batch.append(revision)
                        if len(current_batch) >= batch_size:
                            yield current_batch
                            current_batch = []
                    
                    revisions_yielded += 1
                    self.stats['processed'] += 1
                    
                    if 'timestamp' in revision and revision['timestamp']:
                        if not self._last_updated or revision['timestamp'] > self._last_updated:
                            self._last_updated = revision['timestamp']
                    
                    if sample_limit and revisions_yielded >= sample_limit:
                        if current_batch:  # Don't lose partial batch if hitting limit
                            yield current_batch
                        return

                page_elem.clear()
                
            if batch_size and current_batch:  # Final partial batch
                yield current_batch

        except Exception as e:
            self.logger.error(f"Error parsing XML stream: {str(e)}", exc_info=True)
            self.stats['errors'] += 1
            raise
        
    def _extract_metadata_from_filename(self, file_name: str) -> tuple:
        file_wiki_match = re.match(r'([a-z]+)-', file_name)
        file_wiki = file_wiki_match.group(1) if file_wiki_match else self.wiki_code

        ym = get_year_month_from_filename(file_name)
        if ym:
            try:
                file_date = datetime.strptime(f"{ym}-01", "%Y-%m-%d").date()
                return file_wiki, file_date
            except ValueError:
                pass
        return file_wiki, None

    def _extract_page_data(self, page_elem: etree._Element) -> Optional[Dict[str, Any]]:
        try:
            page_id = page_elem.findtext('./mw:id', namespaces=WIKI_NS) or ""
            title = page_elem.findtext('./mw:title', namespaces=WIKI_NS)
            ns = int(page_elem.findtext('./mw:ns', default='0', namespaces=WIKI_NS))
            is_redirect = page_elem.find('./mw:redirect', namespaces=WIKI_NS) is not None
            
            return ic({
                'page_id': page_id,
                'title': title,
                'namespace': ns,
                'is_redirect': is_redirect,
                'hash_id': hashlib.sha256(f"{self.wiki_code}-{page_id}".encode()).hexdigest(),
                'should_skip': ns != 0
            })
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Invalid page data: {e}")
            self.stats['errors'] += 1
            return None

    def _process_page_revisions(self, page_elem: etree._Element, page_data: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        for rev_elem in page_elem.findall('./mw:revision', namespaces=WIKI_NS):
            revision_data = self._extract_revision_data(rev_elem)
            
            if revision_data:
                # build unified Avro-like record
                page_hash = page_data['hash_id']
                # derive year_month from revision timestamp (in seconds)
                rev_ts = revision_data.get('timestamp', 0)
                year_month = datetime.fromtimestamp(rev_ts, timezone.utc).strftime("%Y-%m") if rev_ts else ''
                wiki_code = page_data['wiki_code']
                yield ic({
                    "page_hash": page_hash,
                    "year_month": year_month,
                    "wiki_code": wiki_code,
                    "tuple_struct": {
                        "hash_id": page_hash,
                        "title": page_data.get('title', ''),
                        "rev_id": revision_data.get('rev_id', ''),
                        "timestamp": datetime.fromtimestamp(revision_data.get('timestamp', 0), timezone.utc).isoformat(),
                        "contributor_id": str(revision_data.get('contributor_id', ''))
                    }
                })
                self.stats['revisions'] += 1
            else:
                self.stats['skipped_revisions'] += 1

    def _extract_revision_data(self, rev_elem: etree._Element) -> Optional[Dict[str, Any]]:
        # drop any revision with no timestamp
        raw_ts = rev_elem.findtext('./mw:timestamp', namespaces=WIKI_NS)
        if not raw_ts:
            # log missing timestamp before skipping
            rev_id = rev_elem.findtext('./mw:id', namespaces=WIKI_NS) or "<unknown>"
            self.logger.warning(f"Dropping revision {rev_id!r} due to missing timestamp")
            return None
        minor_elem = rev_elem.find('./mw:minor', namespaces=WIKI_NS)
        if minor_elem is not None:
            return None
            
        try:
            timestamp = parse_wikimedia_timestamp(
                raw_ts,
                context="revision timestamp",
                logger=self.logger
            )
            if not timestamp:
                return None
                
            contributor_data = self._extract_contributor(rev_elem)
            
            return ic({
                'rev_id': rev_elem.findtext('./mw:id', namespaces=WIKI_NS) or "",
                # convert parsed datetime to epoch seconds (Avro int)
                'timestamp': int(timestamp.timestamp()),
                'contributor_id': contributor_data.get('id') or None
            })
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Invalid revision data: {e}")
            return None

    def _extract_contributor(self, rev_elem: etree._Element) -> Dict[str, Any]:
        contrib = rev_elem.find('./mw:contributor', namespaces=WIKI_NS)
        
        if contrib is None or contrib.get('deleted') == 'deleted':
            return {'id': 0}

        id_text = contrib.findtext('./mw:id', namespaces=WIKI_NS)
        username_text = contrib.findtext('./mw:username', namespaces=WIKI_NS)
        ip_text = contrib.findtext('./mw:ip', namespaces=WIKI_NS)
        return ic({
            'id': int(id_text) if id_text else 0,
            'username': username_text or "",
            'ip': ip_text or ""
        })
    
    @property
    def last_updated(self) -> Optional[datetime]:
        return self._last_updated