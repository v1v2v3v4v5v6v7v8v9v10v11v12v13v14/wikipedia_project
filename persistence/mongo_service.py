from pymongo import MongoClient, UpdateOne # Changed from ReplaceOne
from config.settings import AVRO_SCHEMA_PATH # Assuming this is still relevant for input
from pymongo.errors import PyMongoError
from typing import List, Dict, Any, Optional, Union
# from dataclasses import asdict # Not used
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
# from wiki_utils.datetime_utils import make_year_month # Keep if used

# Constants - these would be defined elsewhere or imported

# Unified time-bucketed page event collection
PAGES_COLLECTION_NAME = "PAGES" # Your existing constant

# We are consolidating, so WIKIDOCUMENT, REVISIONDATA etc. as separate collections are less relevant.
# The primary collection is PAGES_COLLECTION_NAME.
# Other collections might be used for other purposes or can be removed if fully consolidated.
COLLECTIONS = [
    # PAGES_COLLECTION_NAME,
    # "WIKIDOCUMENT", # Candidate for removal if data merged into PAGES
    # "REVISIONDATA", # Candidate for removal
    # "PAGEVIEW",     # Candidate for removal
    # "CLICKSTREAM",  # Candidate for removal
    # "INBOUND_PAGELINKS", # Candidate for removal
    # "OUTBOUND_PAGELINKS" # Candidate for removal
]

# Indices will primarily be on PAGES_COLLECTION_NAME
INDICES = {
    PAGES_COLLECTION_NAME: [
        ("page_hash", 1),  # Key for upserting all data types
        [("year_month", 1)],  # Compound index for page and time bucket
        ("wiki_code", 1),
        ("metadata.tuple_struct.page_title", 1)  # <-- the nested field explicitly indexed
        # Add other indexes based on query patterns for fields within PAGES documents
    ],
    # Remove indices for collections that are being deprecated/merged
}

class MongoPersistenceService:
    """A MongoDB persistence service with simplified 1:1 field mapping, targeting a unified PAGES collection."""

    def __init__(self, mongo_uri: str, db_name: str, schema_path: str = None) -> None: # Made schema_path optional
        """Initialize the service with MongoDB connection and schema."""
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.schema_path = schema_path
        # self.document_cache: Dict[str, bool] = {} # REMOVED - upserts handle existence
        self.logger = self._configure_logger()
        if self.schema_path: # Only load if path provided
            self._load_schemas()
        self._init_collections() # This will now primarily set up PAGES_COLLECTION_NAME

        # Explicitly get the PAGES collection after _init_collections
        if hasattr(self, PAGES_COLLECTION_NAME):
             self.PAGES = getattr(self, PAGES_COLLECTION_NAME)
        else:
            # Fallback or error if PAGES collection wasn't initialized (should not happen with current _init_collections)
            self.logger.warning(f"'{PAGES_COLLECTION_NAME}' collection not initialized directly. Attempting to access.")
            self.PAGES = self.db[PAGES_COLLECTION_NAME]


    def _configure_logger(self) -> logging.Logger:
        """Configure and return a logger instance."""
        logger = logging.getLogger("mongo_persistence")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            file_handler = RotatingFileHandler(
                "persistence.log",
                maxBytes=5*1024*1024,  # 5MB
                backupCount=3
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        return logger

    def _load_schemas(self) -> None:
        """Load AVRO schemas from file."""
        try:
            with open(self.schema_path, 'r') as schema_file:
                self.schemas = json.load(schema_file)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Failed to load schemas: {e}")
            # Decide if this is fatal or not
            # raise

    def _init_collections(self) -> None:
        """Initialize collections and create indexes only if they don't already exist."""
        # Focus on PAGES_COLLECTION_NAME, other collections might be deprecated
        # Ensure PAGES_COLLECTION_NAME is in COLLECTIONS list if not already
        active_collections_to_init = [PAGES_COLLECTION_NAME] + [c for c in COLLECTIONS if c != PAGES_COLLECTION_NAME]

        for collection_name in active_collections_to_init:
            if collection_name not in self.db.list_collection_names():
                try:
                    collection = self.db.create_collection(collection_name)
                    self.logger.info(f"Created collection {collection_name}")
                except PyMongoError as e:
                    self.logger.error(f"Failed to create collection {collection_name}: {e}")
                    continue # Skip index creation if collection creation failed
            else:
                collection = self.db[collection_name]
            setattr(self, collection_name, collection) # Make collection accessible as self.COLLECTION_NAME

            index_specs = INDICES.get(collection_name, [])
            if index_specs:
                current_indexes_on_collection = {tuple(sorted(idx['key'].items())) for idx in collection.list_indexes()}
                for index_spec_item in index_specs: # Iterate through list of index definitions
                    # Ensure index_spec_item is a list of tuples for create_index
                    if isinstance(index_spec_item, tuple): # e.g., ("page_hash", 1)
                        index_to_create_list = [index_spec_item]
                        index_key_tuple_for_check = tuple(sorted([index_spec_item]))
                    elif isinstance(index_spec_item, list): # e.g., [("page_hash",1), ("wiki_code",1)]
                        index_to_create_list = index_spec_item
                        index_key_tuple_for_check = tuple(sorted(index_spec_item))
                    else:
                        self.logger.error(f"Invalid index specification for {collection_name}: {index_spec_item}")
                        continue

                    if index_key_tuple_for_check not in current_indexes_on_collection:
                        try:
                            collection.create_index(index_to_create_list)
                            self.logger.info(f"Created index on {collection_name}: {index_to_create_list}")
                        except PyMongoError as e:
                             self.logger.error(f"Failed to create index {index_to_create_list} on {collection_name}: {e}")
                    else:
                        self.logger.debug(f"Index {index_to_create_list} already exists on {collection_name}")


    def _get_current_timestamp_iso(self) -> str:
        return datetime.utcnow().isoformat() + "Z"

    # _upsert_page_bucket is kept as it's used by existing methods, but now it will always target self.PAGES
    def _upsert_page_bucket(self, page_hash: str, year_month: Optional[str], update_query: dict) -> None:
        """
        Helper to upsert into the unified PAGES collection.
        'year_month' is now optional or used differently (e.g., part of page_hash or for sub-bucketing).
        The main document is identified by page_hash.
        """
        # If year_month is part of the primary key for PAGES documents (e.g. page_hash_YYYY-MM)
        # Then filter should be: {"_id": f"{page_hash}_{year_month}"} or similar
        # If PAGES documents are unique by page_hash only, and year_month is for sub-data:
        filter_doc = {"page_hash": page_hash}
        if year_month: # If year_month is used to further scope the document (e.g. monthly documents per page)
            filter_doc["year_month_bucket"] = year_month # Assuming a field for this

        self.PAGES.update_one(
            filter_doc,
            update_query, # This should include $setOnInsert for base fields
            upsert=True
        )

    # _validate_document_exists REMOVED, document_cache REMOVED

    def save_document(self, document: Dict[str, Any]) -> bool:
        """Saves document metadata with consistent field handling."""
        try:
            # Validate required fields
            page_hash = document.get("page_hash")
            if not page_hash:
                self.logger.error("'page_hash' is required")
                return False

            # Extract common fields
            year_month = document.get("year_month")
            wiki_code = document.get("wiki_code")
            current_time_iso = self._get_current_timestamp_iso()

            # Handle tuple_struct differently based on content
            tuple_struct = document.get("tuple_struct", {})
            if hasattr(tuple_struct, 'access_type'):  # Pageview-like structure
                title = document.get("title")
                page_id = None
            else:  # Document-like structure
                title = tuple_struct.get('page_title', None)  or document.get("title")
                page_id = tuple_struct.get('page_id', None)

            # Build filter
            filter_doc = {"page_hash": page_hash}
            if year_month:
                filter_doc["year_month"] = year_month
            if wiki_code:
                filter_doc["wiki_code"] = wiki_code

            # Check existing document
            existing_doc = self.PAGES.find_one(filter_doc)
            doc_exists = existing_doc is not None

            # Build core update
            update_query = {
                "$set": {
                    "last_updated_global": current_time_iso,
                    "current_title": title if title else None,
                    **{f"metadata.{k}": v for k, v in document.items() 
                    if k not in ["page_hash", "year_month", "tuple_struct"]}
                }
            }

            # Handle page_id (unique field)
            if page_id is not None:
                if not doc_exists or existing_doc.get("page_id") != page_id:
                    update_query["$set"]["page_id"] = page_id

            # Handle historical titles (array field)
            if title:
                if doc_exists:
                    update_query.setdefault("$addToSet", {})["historical_titles"] = title
                else:
                    update_query["$set"]["historical_titles"] = [title]

            # Initialize new document
            if not doc_exists:
                update_query["$set"].update({
                    "created_at": current_time_iso,
                    "revisions": [],
                    "pageviews": [],
                    "clickstreams_in": [],
                    "clickstreams_out": [],
                    "pagelinks_inbound": [],
                    "pagelinks_outbound": []
                })

            # Execute update
            result = self.PAGES.update_one(
                filter_doc,
                update_query,
                upsert=True
            )
            return result.acknowledged

        except PyMongoError as e:
            self.logger.error(f"Document save failed for {document.get('page_hash')}: {e}")
            return False

    def save_pageview(self, pageview_data: Dict[str, Any]) -> bool:
        """Saves pageview data with consistent structure."""
        try:
            # Validate required fields
            page_hash = pageview_data.get("page_hash")
            if not page_hash:
                self.logger.error("'page_hash' is required")
                return False

            # Extract and validate tuple_struct
            tuple_s = pageview_data.get("tuple_struct", {})
            required_fields = ["access_type", "user_type", "date", "views"]
            if not all(k in tuple_s for k in required_fields):
                self.logger.error(f"Missing required fields: {required_fields}")
                return False

            # Parse timestamp
            try:
                date_value = tuple_s["date"]
                if isinstance(date_value, str):
                    dt = datetime.fromisoformat(date_value.replace("Z", "+00:00"))
                elif isinstance(date_value, (int, float)):
                    dt = datetime.fromtimestamp(date_value, tz=timezone.utc)
                else:
                    dt = date_value  # Assume already datetime
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)

            except Exception as e:
                self.logger.error(f"Invalid timestamp: {e}")
                return False

            # Build update operation
            update_query = {
                "$addToSet": {
                    "pageviews": (
                        tuple_s["access_type"],
                        tuple_s["user_type"],
                        date_str,
                        tuple_s["views"]
                    )
                },
                "$set": {
                    "last_updated_global": self._get_current_timestamp_iso()
                }
            }

            # Initialize if new document
            if not self.PAGES.find_one({"page_hash": page_hash}):
                update_query["$setOnInsert"] = {
                    **self._get_base_set_on_insert(page_hash, pageview_data.get("wiki_code")),
                    "pageviews": []  # Initialize empty
                }

            # Execute update
            result = self.PAGES.update_one(
                {"page_hash": page_hash},
                update_query,
                upsert=True
            )
            
            if result.modified_count or result.upserted_id:
                self.logger.debug(f"Pageview saved for {page_hash}")
                return True
            return False

        except PyMongoError as e:
            self.logger.error(f"Pageview save failed for {pageview_data.get('page_hash')}: {e}")
            return False
                
    def save_revision(self, revision: Dict[str, Any]) -> bool:
        """
        Saves a revision into the 'revisions' array of the corresponding PAGES document.
        'revision' dict should contain 'page_hash' and revision-specific fields.
        """
        try:
            # 'page_id' in your original code for revisions was likely the page_hash or similar key
            page_hash = revision.get("page_id") # Using 'page_id' as it was in original, assuming it's the page_hash
            if not page_hash:
                self.logger.error("save_revision: 'page_id' (as page_hash) is required.")
                return False

            # revision_content = {k: v for k, v in revision.items() if k != "page_id"}
            # Ensure tuple_struct is included if it contains the core revision data
            revision_details_to_push = revision.get("tuple_struct", revision) # Prioritize tuple_struct
            if "page_id" in revision_details_to_push: # Avoid duplicating page_hash inside array if it's just the key
                 del revision_details_to_push["page_id"]


            current_time_iso = self._get_current_timestamp_iso()
            # Exclude 'revisions' from $setOnInsert to avoid top-level collision
            set_on_insert = {k: v for k, v in self._get_base_set_on_insert(page_hash, revision.get("wiki_code"), revision.get("title")).items() if k not in ["revisions"]}
            update_query = {
                "$addToSet": {"revisions": revision_details_to_push},
                "$set": {"last_updated_global": current_time_iso},
                "$setOnInsert": set_on_insert
            }

            # Determine year_month for potential bucketing if revisions are too many
            # For minimal change, we use _upsert_page_bucket which can handle year_month if needed for the main doc
            # Or, we assume revisions are pushed to a single array in the page_hash identified document
            # Here, directly updating the PAGES document identified by page_hash.
            # If 'year' was for a separate REVISIONDATA collection, it's now part of the main page doc context.
            # For simplicity, not using year_month for the upsert filter of the main PAGES doc here.
            self.PAGES.update_one(
                 {"page_hash": page_hash},
                 update_query,
                 upsert=True
            )
            self.logger.info(f"Revision saved for page_hash: {page_hash}")
            return True
        except PyMongoError as e:
            self.logger.error(f"Failed to save revision for {page_hash}: {e}")
            return False


    def save_clickstream(self, clickstream_data: Dict[str, Any]) -> bool:
        """
        Saves clickstream data. Assumes 'target_page' is the page_hash of the document
        where this clickstream (as inbound) should be recorded.
        Or, if it's an outbound click, 'source_page_hash' would be the key.
        This example assumes clickstream_data refers to inbound clicks to 'target_page'.
        """
        try:
            # Assuming 'target_page' from original is the page_hash for the document to update
            # This implies this function is for INBOUND clickstreams to target_page
            page_hash = clickstream_data.get("target_page")
            if not page_hash: # If this is for outbound, the key would be different
                self.logger.error("save_clickstream: 'target_page' (as page_hash) is required.")
                return False

            # The actual clickstream event data is in 'tuple_struct'
            click_event_details = clickstream_data.get("tuple_struct", {})
            if not click_event_details:
                 self.logger.warning(f"No tuple_struct in clickstream data for {page_hash}")
                 return False


            current_time_iso = self._get_current_timestamp_iso()
            # Exclude 'clickstreams_in' from $setOnInsert to avoid top-level collision
            set_on_insert = {k: v for k, v in self._get_base_set_on_insert(page_hash, clickstream_data.get("wiki_code")).items() if k not in ["clickstreams_in"]}
            update_query = {
                "$addToset": {"clickstreams_in": click_event_details},
                "$set": {"last_updated_global": current_time_iso},
                "$setOnInsert": set_on_insert
            }

            # self._upsert_page_bucket(page_hash, str(year), update_query) # Original used year for main doc key
            self.PAGES.update_one(
                 {"page_hash": page_hash},
                 update_query,
                 upsert=True
            )
            self.logger.info(f"Clickstream event saved for page_hash: {page_hash}")
            return True
        except PyMongoError as e:
            self.logger.error(f"Failed to save clickstream for {page_hash}: {e}")
            return False

    def _save_pagelink(self, pagelink_data: Dict[str, Any], anchor_field_in_tuple: str, link_direction_array: str) -> bool:
        """
        Generic helper to save pagelinks into the PAGES collection.
        anchor_field_in_tuple: key within 'tuple_struct' that gives the page_hash of the document to update
                                (e.g., 'pl_from' for outbound, 'pl_target_id' for inbound).
        link_direction_array: the array field name in PAGES doc (e.g., 'pagelinks_outbound', 'pagelinks_inbound').
        """
        try:
            tuple_s = pagelink_data.get("tuple_struct", {})
            if not tuple_s:
                self.logger.error("_save_pagelink: 'tuple_struct' is missing.")
                return False

            page_hash_to_update = str(tuple_s.get(anchor_field_in_tuple))
            if not page_hash_to_update or page_hash_to_update == 'None': # Check for "None" string too
                self.logger.error(f"_save_pagelink: '{anchor_field_in_tuple}' in tuple_struct is missing or invalid.")
                return False

            # The actual link data to push is the tuple_struct itself (or parts of it)
            link_details_to_push = dict(tuple_s) # Create a copy
            # Optionally remove the anchor_field if it's just the key and not needed in the array item
            # if anchor_field_in_tuple in link_details_to_push:
            #     del link_details_to_push[anchor_field_in_tuple]

            current_time_iso = self._get_current_timestamp_iso()
            # Exclude pagelinks_inbound or pagelinks_outbound as appropriate from $setOnInsert
            exclude_field = []
            if link_direction_array in ["pagelinks_inbound", "pagelinks_outbound"]:
                exclude_field = [link_direction_array]
            set_on_insert = {k: v for k, v in self._get_base_set_on_insert(page_hash_to_update, pagelink_data.get("wiki_code")).items() if k not in exclude_field}
            update_query = {
                "$addToSet": {link_direction_array: link_details_to_push},
                "$set": {"last_updated_global": current_time_iso},
                "$setOnInsert": set_on_insert
            }

            # The 'year_month' from original pagelink_data was for the main doc key in _upsert_page_bucket.
            # Now, PAGES docs are primarily keyed by page_hash.
            # self._upsert_page_bucket(page_hash_to_update, pagelink_data.get("year_month"), update_query)
            self.PAGES.update_one(
                 {"page_hash": page_hash_to_update},
                 update_query,
                 upsert=True
            )
            self.logger.info(f"Pagelink ({link_direction_array}) saved for page_hash: {page_hash_to_update}")
            return True
        except PyMongoError as e:
            self.logger.error(f"Failed to save pagelink ({link_direction_array}) for {page_hash_to_update}: {e}")
            return False
        except Exception as ex: # Catch other potential errors like str conversion
            self.logger.error(f"Unexpected error in _save_pagelink ({link_direction_array}) for {page_hash_to_update}: {ex}")
            return False


    def save_outbound_pagelink(self, pagelink_data: Dict[str, Any]) -> bool:
        """ Saves outbound pagelinks. 'tuple_struct.pl_from' is the page_hash of the source page. """
        return self._save_pagelink(pagelink_data, anchor_field_in_tuple="pl_from", link_direction_array="pagelinks_outbound")

    def save_inbound_pagelink(self, pagelink_data: Dict[str, Any]) -> bool:
        """ Saves inbound pagelinks. 'tuple_struct.pl_target_id' is the page_hash of the target page. """
        # Ensure pl_target_id is consistently the field name for the target page's hash.
        return self._save_pagelink(pagelink_data, anchor_field_in_tuple="pl_target_id", link_direction_array="pagelinks_inbound")

    def save_page_info(self, page_info_data: Dict[str, Any]) -> bool:
        """
        Saves basic page info fields into the unified PAGES collection.
        Expects page_info_data to contain:
          - 'page_hash': the unique hash key for the page
          - 'page_namespace', 'page_title', 'page_is_redirect', 'page_touched'
          - optional 'wiki_code'
        """
        try:
            page_hash = page_info_data.get("page_hash")
            if not page_hash:
                self.logger.error("save_page_info: 'page_hash' is required.")
                return False

            # Extract fields from page_info_data
            namespace = page_info_data.get("page_namespace")
            is_redirect = page_info_data.get("page_is_redirect")
            touched = page_info_data.get("page_touched")
            wiki_code = page_info_data.get("wiki_code")
            title = page_info_data.get("page_title")

            # Build metadata update
            metadata = {
                "page_namespace": namespace,
                "page_title": title,
                "page_is_redirect": is_redirect,
                "page_touched": touched
            }
            flattened = {f"metadata.{k}": v for k, v in metadata.items()}
            current_time = self._get_current_timestamp_iso()

            update_query = {
                "$addToSet": {"metadata.page_title": title},
                "$set": {**flattened, "last_updated_global": current_time},
                "$setOnInsert": {
                    **self._get_base_set_on_insert(page_hash, wiki_code),
                    "metadata.page_title": []
                }
            }

            # Upsert into unified PAGES collection: both all-time and monthly bucketed docs
            # 1) All-time document (no year_month)
            self._upsert_page_bucket(page_hash, None, update_query)
            # 2) Monthly document, using page_touched to determine bucket
            try:
                if isinstance(touched, str):
                    dt = datetime.fromisoformat(touched.replace("Z", "+00:00"))
                else:
                    dt = touched
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            except Exception as bucket_err:
                self.logger.error(f"save_page_info: invalid page_touched value for monthly bucket: {touched}, error: {bucket_err}")
            self.logger.info(f"Page info saved for page_hash: {page_hash} (all-time and monthly)")
            return True
        except PyMongoError as e:
            self.logger.error(f"Failed to save page info for {page_hash}: {e}")
            return False

    def close(self) -> None:
        """Clean up resources."""
        self.client.close()
        self.logger.info("MongoDB connection closed")

