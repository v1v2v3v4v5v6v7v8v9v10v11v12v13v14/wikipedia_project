from pymongo import MongoClient, ReplaceOne

"""
This module implements a MongoDB persistence service with separate methods for saving documents and revisions.

Persistence Strategies:

1. Separate inserts without validation (Rating: Low)
   - Directly inserts documents and revisions without verifying document existence.
   - Risk: May lead to orphaned revisions if the main document is missing.

2. Separate inserts with validation using caching (Rating: Medium-High) [Current Choice]
   - Uses a simple caching mechanism to validate document existence before inserting revisions.
   - Balance: Offers improved performance while maintaining reasonable data integrity.

3. Separate inserts with periodic reconciliation (Rating: Medium)
   - Inserts documents and revisions without immediate validation, with integrity checks performed periodically.
   - Trade-off: Simpler implementation but delayed detection of orphaned revisions.

4. Using database transactions (Rating: High)
   - Employs database transactions to ensure atomicity and strict data integrity.
   - Trade-off: Highest data consistency at the cost of performance and increased complexity.
"""

# error handling
# configurable testing
# testing

from wiki_utils.datetime_utils import make_year_month

from typing import List, Dict, Any, Union
from dataclasses import asdict
from constants import INDICES
from wiki_utils.datetime_utils import make_year_month
from config.settings import PERSISTENCE_LOGFILE_PATH
import logging


class MongoPersistenceService:
    def __init__(self, mongo_uri: str, db_name: str) -> None:
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self._init_collections()
        # Initialize a simple cache to store document existence, reducing repeated database queries.
        self.document_cache = {}
        self.logger = self.get_logger("mongo_logger")

    def get_logger(
        self,
        name: str,
        write_to_file: bool = True,
        logfile_path: bool = PERSISTENCE_LOGFILE_PATH,
    ) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s -%(name)s -%(levelname)s -%(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        if write_to_file:
            logger.handlers.append(logging.FileHandler(logfile_path))

        return logger

    def _init_collections(self):
        """Initialize collections and indexes from schema config"""
        for collection_name in FIELD_MAPPINGS.keys():
            # Create collection reference
            setattr(self, collection_name, self.db[collection_name])

            # Create indexes
            for field in INDICES.get(collection_name, []):
                getattr(self, collection_name).create_index(field)
                
    def _map_value(self, source: Union[str, tuple], data: Any) -> Any:
        """
        Extract value from source data
        - If source is string: get from dict field
        - If source is tuple: (position, transform_fn)
        """
        if isinstance(source, tuple):
            value = (
                data[source[0]]
                if isinstance(data, (list, tuple))
                else data.get(source[0])
            )
            return source[1](value) if source[1] else value
        return data.get(source)

    def _map_document(self, data: Any, collection: str) -> Dict:
        """Create MongoDB document using schema mapping"""
        source_data = asdict(data) if hasattr(data, "to_dict") else data
        return {
            mongo_field: self._map_value(source, source_data)
            for mongo_field, source in FIELD_MAPPINGS[collection].items()
        }

    def save_document_only(self, document: Any) -> Dict:
        """
        Save only the main document without handling revisions.
        This method maps the document using the WIKIDOCUMENT schema,
        inserts or updates it in the database, and updates the document cache.
        """
        # Map the document data using the WIKIDOCUMENT schema
        doc_data = self._map_document(document, "WIKIDOCUMENT")
        # Insert or update the main document in the collection
        self.WIKIDOCUMENT.replace_one({"_id": doc_data["_id"]}, doc_data, upsert=True)
        # Update the document cache to reflect that the document now exists.
        self.document_cache[doc_data["doc_id"]] = True
        return doc_data

    def save_document(self, document: Any) -> bool:
        """Save document and its revisions"""
        try:
            # Save the main document using the dedicated method.
            doc_data = self.save_document_only(document)

            # Save revisions if they exist by delegating to save_revision.
            # This separation ensures that document saving and revision saving are handled independently.
            if hasattr(document, "revisions"):
                revs = (
                    asdict(document).get("revisions", [])
                    if hasattr(document, "to_dict")
                    else document.get("revisions", [])
                )
                for rev in revs:
                    self.save_revision(rev, doc_data)

            return True
        except Exception as e:
            self.logger.error(f"Error saving document: {str(e)}")
            return False

    def save_revision(self, rev: tuple, doc_data: Dict) -> bool:
        """
        Save a single revision for a document.
        Performs a lightweight validation to ensure the parent document exists using caching.
        """
        try:
            # Extract document ID from the provided document data.
            doc_id = doc_data["doc_id"]

            # Validate the existence of the document using the cache.
            if doc_id not in self.document_cache:
                # If not in cache, perform a database query to check existence.
                # This is a lightweight check to avoid saving revisions for non-existent documents.
                if not self.WIKIDOCUMENT.find_one({"doc_id": doc_id}):
                    self.logger.error(
                        f"Validation failed: Document with doc_id {doc_id} does not exist."
                    )
                    return False
                # Update cache after successful validation.
                self.document_cache[doc_id] = True

            # Map revision data to the required format using REVISIONDATA schema
            rev_data = {
                "_id": rev[2],
                "doc_id": doc_data["doc_id"],
                "rev_id": rev[2],
                "ts": rev[0],
                "eid": rev[1],
                "wiki_code": doc_data["wiki_code"],
                "year_month": make_year_month(rev[0]),
            }

            # Save the revision document in the revisions collection.
            self.REVISIONDATA.replace_one(
                {"_id": rev_data["_id"]}, rev_data, upsert=True
            )
            return True
        except Exception as e:
            self.logger.error(f"Error saving revision: {str(e)}")
            return False

    def save_pageview(self, pageview_data: Any) -> bool:
        """Save pageview data"""
        try:
            # Map pageview data according to PAGEVIEW schema
            pageview_doc = self._map_document(
                pageview_data, "PAGEVIEW"
            )  # Using PAGEVIEW schema
            # Validate existence of associated document using caching strategy.
            doc_id = pageview_doc.get("doc_id")
            if doc_id not in self.document_cache:
                # If document not in cache, check in main document collection.
                if not self.WIKIDOCUMENT.find_one({"doc_id": doc_id}):
                    self.logger.INFO(
                        f"Validation failed: Document with doc_id {doc_id} does not exist."
                    )
                    return False
                # Update cache after successful validation.
                self.document_cache[doc_id] = True
            # Insert or update the pageview document in the collection
            self.PAGEVIEW.replace_one(
                {"_id": pageview_doc["_id"]}, pageview_doc, upsert=True
            )
            return True
        except Exception as e:
            self.logger.error(f"Error saving pageview: {str(e)}")
            return False

    def save_clickstream(self, clickstream_data: Any) -> bool:
        """Save clickstream data"""
        try:
            # Map clickstream data according to CLICKSTREAM schema
            clickstream_doc = self._map_document(
                clickstream_data, "CLICKSTREAM"
            )  # Using CLICKSTREAM schema
            # Validate existence of associated document using caching strategy.
            doc_id = clickstream_doc.get("doc_id")
            if doc_id not in self.document_cache:
                # If document not in cache, check in main document collection.
                if not self.WIKIDOCUMENT.find_one({"doc_id": doc_id}):
                    self.logger.INFO(
                        f"Validation failed: Document with doc_id {doc_id} does not exist."
                    )
                    return False
                # Update cache after successful validation.
                self.document_cache[doc_id] = True
            # Insert or update the clickstream document in the collection
            self.CLICKSTREAM.replace_one(
                {"_id": clickstream_doc["_id"]}, clickstream_doc, upsert=True
            )
            return True
        except Exception as e:
            self.logger.error(f"Error saving clickstream: {str(e)}")
            return False

    def save_pagelink(self, pagelink_data: Any) -> bool:
        """Save pagelink data"""
        try:
            # Map pagelink data according to PAGELINK schema
            pagelink_doc = self._map_document(
                pagelink_data, "PAGELINK"
            )  # Using PAGELINK schema
            # Validate existence of associated document using caching strategy.
            doc_id = pagelink_doc.get("doc_id")
            if doc_id not in self.document_cache:
                # If document not in cache, check in main document collection.
                if not self.WIKIDOCUMENT.find_one({"doc_id": doc_id}):
                    self.logger.error(
                        f"Validation failed: Document with doc_id {doc_id} does not exist."
                    )
                    return False
                # Update cache after successful validation.
                self.document_cache[doc_id] = True
            # Insert or update the pagelink document in the collection
            self.PAGELINK.replace_one(
                {"_id": pagelink_doc["_id"]}, pagelink_doc, upsert=True
            )
            return True
        except Exception as e:
            self.logger.error(f"Error saving pagelink: {str(e)}")
            return False
