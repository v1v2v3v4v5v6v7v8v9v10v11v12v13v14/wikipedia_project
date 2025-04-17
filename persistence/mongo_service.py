# From: WikipediaDocumentStore class
# Current Location: src/wikipedia_documents/wikipedia_document.py
# Target Location: Logic moves primarily to persistence/providers/mongo/service.py

def __init__(self, mongo_uri: str = "mongodb://localhost:27017",
             db_name: str = "wikipedia"):
    # --- Analysis ---
    # Purpose: Initializes MongoDB connection, database, collections, and instantiates a parser.
    # Logic: Standard pymongo connection. Instantiates parser directly. Calls index init.
    # Dependencies: MongoClient, WikipediaParser.
    # --- Refactoring Notes ---
    # - Parser Instantiation: REMOVE `self.parser = WikipediaParser()`. The store/service should not manage parsing.
    # - Connection Logic: Keep Mongo connection logic (client, db, collections) but move it to the
    #   constructor of the new MongoPersistenceService.
    # - Index Init Call: Keep the call to _init_collections, but move it within MongoPersistenceService.
    # --- Target Framework ---
    # - Connection/collection attributes move to MongoPersistenceService (__init__).
    # - Parser instantiation is removed.
    # - Call to _init_collections moves to MongoPersistenceService (__init__).
    self.client = MongoClient(mongo_uri)
    self.db = self.client[db_name]
    self.documents = self.db.documents
    self.revisions = self.db.revisions
    self.parser = WikipediaParser() # <<< REMOVE THIS LINE
    self._init_collections()


# From: WikipediaDocumentStore class
# Current Location: src/wikipedia_documents/wikipedia_document.py
# Target Location: persistence/providers/mongo/service.py

def _init_collections(self):
    """Ensure indexes exist"""
    # --- Analysis ---
    # Purpose: Creates MongoDB indexes on the documents collection.
    # Logic: Calls create_index for several fields, including embedded revision fields.
    # Dependencies: self.documents (pymongo collection object).
    # --- Refactoring Notes ---
    # - Move this logic into the MongoPersistenceService, likely called from its __init__.
    # - Re-evaluate Indexes: Critically assess if indexes on `revisions.rev_id` and `revisions.ts`
    #   within the `documents` collection are needed, especially if revisions are primarily queried
    #   from the separate `revisions` collection. They could add significant overhead to document writes.
    # - Revision Collection Indexes: This method *only* indexes `documents`. The `revisions` collection
    #   also needs indexes (e.g., on `rev_id`, `doc_id`, `ts`). This logic is missing.
    # --- Target Framework ---
    # - Move index creation logic to MongoPersistenceService (__init__ or a setup method).
    # - REMOVE or justify the `revisions.*` indexes on the `documents` collection.
    # - ADD index creation logic for the `revisions` collection (e.g., self.revisions.create_index(...)).
    self.documents.create_index("_id") # Usually automatic, but explicit is okay.
    self.documents.create_index("doc_id")
    self.documents.create_index("wiki_code")
    self.documents.create_index("title")
    self.documents.create_index("revisions.rev_id") # <<< REVIEW / POTENTIALLY REMOVE
    self.documents.create_index("revisions.ts")     # <<< REVIEW / POTENTIALLY REMOVE
    # --- MISSING: Index creation for self.revisions collection ---


# From: WikipediaDocumentStore class
# Current Location: src/wikipedia_documents/wikipedia_document.py
# Target Location: Logic related to storage moves to persistence/providers/mongo/service.py. Parsing part is removed.

def process_and_store(self, raw_data: Dict) -> bool:
    """Process raw data and store in MongoDB"""
    # --- Analysis ---
    # Purpose: Parses raw data AND stores the resulting document and its revisions.
    # Logic: Calls the problematic WikipediaDocument.from_parser. Uses replace_one for the doc.
    #        Attempts to bulk_write revisions but uses incorrect logic for tuples. Catches generic Exception.
    # Dependencies: WikipediaDocument.from_parser, self.parser, self.documents, self.revisions.
    # --- Refactoring Notes ---
    # - MAJOR ISSUE (SRP): Combines parsing and storage. Parsing must be removed.
    # - Input: This method (in the new MongoPersistenceService) should accept an already instantiated
    #   `WikipediaDocument` object or a dictionary ready for storage.
    # - Document Storage: Use `ReplaceOne({"_id": document.hash_id}, document_dict, upsert=True)` for clarity.
    # - Revision Storage BUG: The logic `UpdateOne(..., {"$set": {**rev, ...}})` is wrong for tuples.
    #   Needs to be rewritten to construct a dict for each revision and use ReplaceOne/UpdateOne correctly
    #   targeting the `revisions` collection (e.g., `ReplaceOne({"rev_id": rev_id, "doc_id": doc_id}, rev_dict, upsert=True)`).
    # - Separate Collections: Logic clearly separates doc and revision writes, which is good.
    # - Error Handling: Catches generic Exception, consider more specific catches if possible.
    # --- Target Framework ---
    # - Remove the try/except block related to `from_parser`.
    # - Adapt function signature to accept processed data (e.g., `def save_document_and_revisions(self, document: WikipediaDocument)`).
    # - Move the `documents.replace_one` logic (adapted) to MongoPersistenceService.
    # - Completely rewrite the `revisions.bulk_write` logic with correct operations targeting the `revisions` collection,
    #   move it to MongoPersistenceService.
    try:
        # --- REMOVE PARSING SECTION ---
        # document = WikipediaDocument.from_parser(self.parser, raw_data) # <<< REMOVE
        # --- END REMOVE ---

        # --- ADAPT TO USE PRE-PROCESSED document OBJECT ---
        # Example assumes 'document' is passed in
        document_dict = document.to_mongo_dict() # Or use logic moved from here
        doc_result = self.documents.replace_one( # Use ReplaceOne
            {"_id": document.hash_id},
            document_dict, # Pass the dict
            upsert=True
        )

        # --- REWRITE REVISION LOGIC ---
        if document.revisions:
            rev_operations = []
            for rev_tuple in document.revisions: # Iterate tuples
                # Example: Assuming rev_tuple = (ts, eid, rev_id)
                ts, eid, rev_id = rev_tuple[0], rev_tuple[1], rev_tuple[2]
                rev_doc = { # Construct the document for the revisions collection
                    "ts": ts,
                    "eid": eid,
                    "rev_id": rev_id,
                    "doc_id": document.doc_id, # Add foreign key
                    "wiki_code": document.wiki_code, # Add other useful info maybe
                    # Add other revision-specific fields if available/needed
                }
                # Use ReplaceOne based on rev_id+doc_id to handle potential duplicates
                rev_operations.append(
                    ReplaceOne(
                        {"rev_id": rev_id, "doc_id": document.doc_id}, # Filter
                        rev_doc,                                     # Replacement doc
                        upsert=True
                    )
                )
            # --- END REWRITE ---
            if rev_operations: # Check if there are ops before writing
                rev_result = self.revisions.bulk_write(rev_operations)

        return doc_result.acknowledged # Or better success check

    except Exception as e:
        # Adapt logging for persistence context
        logging.error(f"Error storing document/revisions for doc_id {getattr(document,'doc_id','N/A')}: {str(e)}")
        return False
    
# From: WikipediaDocumentStore class
# Current Location: src/wikipedia_documents/wikipedia_document.py
# Target Location: Logic related to storage moves to persistence/providers/mongo/service.py. Parsing part is removed.

def bulk_process_and_store(self, raw_data_list: List[Dict]) -> Dict:
    """Bulk process and store documents"""
    # --- Analysis ---
    # Purpose: Parses and stores batches of documents and revisions.
    # Logic: Loops through raw data, calls from_parser, builds lists of operations for docs and revisions,
    #        executes two bulk_write calls. Handles BulkWriteError.
    # Dependencies: WikipediaDocument.from_parser, self.parser, self.documents, self.revisions.
    # --- Refactoring Notes ---
    # - MAJOR ISSUE (SRP): Same parsing/storage mixing as single process method. Parsing loop must be removed.
    # - Input: This method (in new MongoPersistenceService) should accept a list of already instantiated
    #   `WikipediaDocument` objects or dicts ready for storage.
    # - Document Storage Op: Use `ReplaceOne` instead of `UpdateOne` with `$set` for clarity.
    # - Revision Storage BUG: Same bug as single process method (`**rev`). Needs complete rewrite.
    # - Error Handling: Good handling of BulkWriteError. Generic Exception catch is okay.
    # - Stats: Stat tracking is useful.
    # --- Target Framework ---
    # - Remove the loop calling `from_parser`.
    # - Adapt function signature (e.g., `def save_documents_and_revisions_bulk(self, documents: List[WikipediaDocument])`).
    # - Loop through the input `documents` list.
    # - Move the logic creating `doc_operations` (using ReplaceOne) and `rev_operations` (corrected logic)
    #   and executing the `bulk_write` calls into the MongoPersistenceService method.
    stats = { # Keep stats logic
        "processed": 0, # This will now be len(documents) passed in
        "stored": 0,
        "errors": 0 # Errors should now primarily reflect write errors
    }

    try:
        doc_operations = []
        rev_operations = []

        # --- REMOVE PARSING LOOP ---
        # for raw_data in raw_data_list: # <<< REMOVE
        #    try:
        #        document = WikipediaDocument.from_parser(self.parser, raw_data) # <<< REMOVE
        # --- END REMOVE ---

        # --- ADAPT TO LOOP THROUGH PRE-PROCESSED documents LIST ---
        # Example assumes 'documents' list is passed in
        stats['processed'] = len(documents) # Update processed count

        for document in documents:
             # --- Document Operation ---
             doc_dict = document.to_mongo_dict() # Or use logic moved from here
             doc_operations.append(
                 ReplaceOne( # Use ReplaceOne
                     {"_id": document.hash_id},
                     doc_dict,
                     upsert=True
                 )
             )

             # --- REWRITE REVISION LOGIC ---
             if document.revisions:
                 for rev_tuple in document.revisions:
                     ts, eid, rev_id = rev_tuple[0], rev_tuple[1], rev_tuple[2]
                     rev_doc = {
                         "ts": ts, "eid": eid, "rev_id": rev_id,
                         "doc_id": document.doc_id, "wiki_code": document.wiki_code
                     }
                     rev_operations.append(
                         ReplaceOne(
                             {"rev_id": rev_id, "doc_id": document.doc_id},
                             rev_doc,
                             upsert=True
                         )
                     )
             # --- END REWRITE ---

        # --- Keep Bulk Write Logic ---
        if doc_operations:
            doc_result = self.documents.bulk_write(doc_operations, ordered=False) # Consider ordered=False
            # Refine stored count logic based on bulk result attributes
            stats["stored"] = doc_result.matched_count + doc_result.upserted_count

        if rev_operations:
            # Consider separate error handling/stats for revision writes
            rev_result = self.revisions.bulk_write(rev_operations, ordered=False) # Consider ordered=False

        return stats

    except BulkWriteError as bwe:
        # Keep BulkWriteError handling, adjust logging context
        logging.error(f"Persistence bulk write error: {bwe.details}")
        stats["errors"] += len(bwe.details.get('writeErrors', [])) # Safely get errors
        # Potentially add more details to stats about which type failed
        return stats
    except Exception as e:
        # Adapt logging context
        logging.error(f"Error in bulk persistence: {str(e)}")
        stats["errors"] += stats['processed'] - stats['stored'] # Estimate errors
        return stats
    

# From: PageviewProcessor class
# Current Location: src/wikipedia_pageviews/processor.py
# Target Location: persistence/providers/mongo/service.py (or persistence/stores/pageview_store.py)

    def write_pageviews_to_mongodb(
        self,
        pageview_batches: Generator[List[Dict[str, Any]], None, None],
        collection_name: str = "page_pageview_tuples"
    ) -> Dict:
        # --- Analysis ---
        # Purpose: Takes batches of processed pageview records and writes them to MongoDB using $addToSet.
        # Logic: Connects to Mongo, iterates batches, constructs UpdateOne operations with $addToSet
        #        to add view tuples to potentially existing documents keyed by page hash. Uses bulk_write.
        # Dependencies: time, datetime, pymongo (MongoClient, UpdateOne, errors), self.mongo_uri,
        #               self.mongo_db, self.log.
        # --- Refactoring Notes ---
        # - This is purely persistence logic. Belongs in the persistence layer.
        # - It takes processed data (batches) and interacts directly with MongoDB.
        # - The schema using `_id` as `page_hash` and an array of `views` tuples with `$addToSet` is a
        #   specific aggregation strategy decided here. This might be better defined closer to the storage layer.
        # - Needs to handle MongoDB connection properly (e.g., connection pooling, closing). Creating a new
        #   client inside the function is generally inefficient; the service should manage the client.
        # - Error handling is basic. BulkWriteError details could be logged more effectively.
        # --- Target Framework ---
        # - Move this logic to a method within `persistence/providers/mongo/service.py` (e.g., `save_pageview_batch`).
        # - The service class will manage the MongoClient instance.
        # - The method will receive the batch and collection name (or use a pre-configured one).
        # - Refine error logging and potentially stats reporting.
        # ... (Code structure needs adaptation for service class context) ...


# From: WikipediaProcessor class
# Current Location: src/wikipedia_revisions/processor.py
# Target Location: persistence/providers/mongo/service.py

    def setup_mongodb_connection(self, collection_names: List[str]) -> Tuple[Optional[Any], Optional[Dict[str, Collection]], bool]:
        """ Establish MongoDB connection and get specified collections. """
        # --- Analysis ---
        # Purpose: Connects to real or mock MongoDB, gets database handle, gets collection objects. Handles basic errors.
        # Logic: Uses self.use_mock flag. Instantiates MongoClient or MockMongoClient. Sets timeouts/write concern. Basic error handling.
        # Dependencies: pymongo, mongomock (optional), self.use_mock, self.mongo_uri, self.mongo_db, self.log.
        # --- Refactoring Notes ---
        # - This is core persistence connection logic.
        # - Should be part of the `MongoPersistenceService` class's initialization or a dedicated connection method within it.
        # - `use_mock` should be handled via dependency injection in testing, not a flag in production code. The service should always try to connect to the configured URI.
        # - Connection parameters (timeouts, appname, write concern) are good.
        # - Error handling is reasonable. Closing client on error is good.
        # --- Target Framework ---
        # - Move this logic into `persistence/providers/mongo/service.py`. Likely called within its `__init__` or a dedicated `_connect` method.
        # - Remove the `self.use_mock` branching; mock connections are handled by injecting a mock client during testing.
        # ... (Code structure is mostly okay, needs context change) ...


# From: WikipediaProcessor class
# Current Location: src/wikipedia_revisions/processor.py
# Target Location: persistence/providers/mongo/service.py (or potentially operations.py)

    def _prepare_mongo_op(self, page_h: str, rev_data: Dict[str, Any], timestamp_dt: datetime) -> Optional[UpdateOne]:
        """ Prepare MongoDB update operation... """
        # --- Analysis ---
        # Purpose: Creates a PyMongo `UpdateOne` operation object to add a revision tuple to a page document using $addToSet.
        # Logic: Constructs the revision tuple, creates the UpdateOne op targeting the page hash (`_id`), uses $addToSet for the tuple, sets `last_updated`/`latest_rev` fields.
        # Dependencies: pymongo.operations.UpdateOne, datetime.
        # --- Refactoring Notes ---
        # - This function defines *how* revision data is structured and stored in MongoDB (as tuples within a page doc). This is persistence logic.
        # - The use of `$addToSet` implies duplicate revision tuples for the same page are okay to ignore (which is generally true).
        # - Setting `last_updated` and `latest_rev` on the main page document is a specific denormalization choice made here.
        # - This logic should be encapsulated within the persistence layer.
        # --- Target Framework ---
        # - Move this logic into `persistence/providers/mongo/service.py`. It might be a private helper method used by the main save/bulk save method, or potentially abstracted further in `operations.py`.
        # - It will take the necessary data (page hash, revision dict, timestamp) and return the PyMongo operation object.
        # ... (Code logic seems mostly sound, assuming the tuple storage strategy is desired) ...

# From: WikipediaProcessor class
# Current Location: src/wikipedia_revisions/processor.py
# Target Location: persistence/providers/mongo/service.py

    def write_ops_batch_to_mongo(self, operations: list, collection: Collection) -> dict:
        # --- Analysis ---
        # Purpose: Executes a batch of PyMongo operations using `bulk_write`. Handles errors and collects stats.
        # Logic: Takes list of operations and collection object. Calls `bulk_write(ordered=False)`. Parses `BulkWriteError` details. Logs extensively. Returns stats dict.
        # Dependencies: pymongo (Collection, BulkWriteError), time, self.log.
        # --- Refactoring Notes ---
        # - Core MongoDB batch writing logic. Belongs in the persistence layer.
        # - Excellent, detailed error handling and logging for `BulkWriteError`.
        # - Using `ordered=False` is good for throughput when individual operation order doesn't matter.
        # - Stats collection is useful.
        # --- Target Framework ---
        # - Move this method into `persistence/providers/mongo/service.py`. It will be a key method used by higher-level save functions.
        # - Adapt to use injected logger.
        # ... (Code logic is robust and well-implemented) ...