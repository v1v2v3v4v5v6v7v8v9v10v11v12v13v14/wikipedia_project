"""
Utilities for generating consistent identifiers and hashes for Wikimedia content
"""

import hashlib
import base64
import urllib.parse
import logging
from typing import Optional

class WikimediaIdentifiers:
    """
    Generator for various Wikimedia identifier schemes
    
    Usage:
        ids = WikimediaIdentifiers()
        page_hash = ids.create_page_identifier("enwiki", "Main_Page")
        doc_hash = ids.generate_document_hash("enwiki", "12345", timestamp)
    """
    
    @staticmethod
    def create_page_identifier(wiki_code: str, page_title: str) -> str:
        """
        Generate a URL-safe hash identifier for a wiki page
        
        Args:
            wiki_code: Wiki project code (e.g. 'enwiki')
            page_title: Page title (unescaped)
            
        Returns:
            Base64-encoded SHA256 hash (URL-safe, 22 chars)
        """
        encoded_title = urllib.parse.quote(page_title)
        hash_input = f"{wiki_code}:{encoded_title}"
        hash_obj = hashlib.sha256(hash_input.encode('utf-8'))
        return base64.urlsafe_b64encode(hash_obj.digest()).decode('utf-8').rstrip('=')[:22]

    @staticmethod
    def generate_content_hash(
        wiki_code: str, 
        content_id: str, 
        logger: Optional[logging.Logger] = None
    ) -> Optional[str]:
        """
        Generate SHA256 hash from wiki code and content ID
        
        Args:
            wiki_code: Wiki project code
            content_id: Page or revision ID
            logger: Optional logger for error reporting
            
        Returns:
            Hex digest of SHA256 hash or None if inputs invalid
        """
        if not all([wiki_code, content_id]):
            if logger:
                logger.warning("Missing required fields for hash generation")
            return None
            
        hash_input = f"{wiki_code}-{content_id}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

    @staticmethod
    def generate_document_hash(
        wiki_code: str,
        doc_id: str,
        timestamp: str,
        logger: Optional[logging.Logger] = None
    ) -> Optional[str]:
        """
        Generate document hash including timestamp
        
        Args:
            wiki_code: Wiki project code
            doc_id: Document ID
            timestamp: ISO format timestamp
            logger: Optional logger
            
        Returns:
            Hex digest of combined hash or None if invalid
        """
        if not all([wiki_code, doc_id, timestamp]):
            if logger:
                logger.warning("Missing required fields for document hash")
            return None
            
        hash_input = f"{wiki_code}-{doc_id}-{timestamp}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

    @staticmethod
    def short_hash(input_str: str, length: int = 8) -> str:
        """
        Generate a short hash for display purposes
        
        Args:
            input_str: String to hash
            length: Desired output length (max 32)
            
        Returns:
            First N chars of SHA256 hash
        """
        full_hash = hashlib.sha256(input_str.encode('utf-8')).hexdigest()
        return full_hash[:min(length, 32)]