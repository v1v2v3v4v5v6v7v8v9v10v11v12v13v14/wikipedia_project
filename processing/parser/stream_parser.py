# From: PageviewProcessor class
# Current Location: src/wikipedia_pageviews/processor.py
# Target Location: processing/parser/pageview_parser.py (or similar config/constants)

    def load_excluded_prefixes(self) -> Set[str]:
        """ Get prefixes to filter out non-article pages. """
        # --- Analysis ---
        # Purpose: Loads a set of page title prefixes used for filtering pageview records.
        # Logic: Reads from environment variable "PREFIXES" or uses a hardcoded default set.
        # Dependencies: os.
        # --- Refactoring Notes ---
        # - This provides configuration data relevant to parsing/filtering.
        # - Reading from environment variables is a valid configuration method.
        # - The hardcoded list acts as a default.
        # - This doesn't need to be an instance method (`self` is not used).
        # - Should be loaded once where needed, likely during the initialization of the parsing component.
        # --- Target Framework ---
        # - Move this logic into the module responsible for pageview record parsing/filtering
        #   (e.g., `processing/parser/pageview_parser.py` or `pageview_handler.py`). It could be a
        #   top-level function or loaded into a constant within that module during import.
        prefixes_env = os.environ.get("PREFIXES", "")
        if prefixes_env:
            return {prefix.strip() for prefix in prefixes_env.split(',') if prefix.strip()}
        # ... hardcoded defaults ...

    # From: PageviewProcessor class
# Current Location: src/wikipedia_pageviews/processor.py
# Target Location: processing/parser/pageview_parser.py (or pageview_handler.py)

    def extract_pageview_record(
        self, line: str, filename: str, timestamp: str = None,
        wiki_filter: Optional[Set[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """ Parse a pageview record line... """
        # --- Analysis ---
        # Purpose: The core parsing function for a single line of pageview data. Handles multiple formats,
        #          extracts fields, applies filtering (wiki code, prefixes, zero views), assigns types.
        # Logic: Splits line, uses conditional logic based on parts length, calls helpers
        #        (analyze_page_name, extract_user_type_from_filename), uses regex for hourly views,
        #        applies filters, constructs result dictionary. Quite complex.
        # Dependencies: re, PageviewProcessor.ACCESS_TYPE_ENUM, PageviewProcessor.USER_TYPE_ENUM,
        #               self.log, self.filter_counts, self.analyze_page_name, self.extract_user_type_from_filename.
        # --- Refactoring Notes ---
        # - This is the heart of the parsing logic. It belongs in a dedicated parser/handler.
        # - It relies heavily on helper methods (`analyze_page_name`, `extract_user_type...`) which should also move with it.
        # - It uses class-level ENUMs, which should be moved near this function.
        # - It updates `self.filter_counts`. The parser/handler should perhaps *return* filter reasons or counts
        #   instead of modifying shared state, or receive a stats dictionary to update.
        # - It calls `self.log`. The new parser/handler class should have its own injected logger.
        # - The logic for handling different numbers of parts (len(parts)) could potentially be simplified or made more robust.
        # - The dependency on `filename` to extract user type tightly couples parsing to filename conventions.
        # --- Target Framework ---
        # - Move this method and its direct helpers (`analyze_page_name`, `extract_user_type...`, `classify_line_pattern`)
        #   to a new class, e.g., `PageviewParser` or `PageviewRecordHandler`, within `processing/parser/`.
        # - Pass `wiki_filter` and `excluded_prefixes` (loaded separately) as arguments or during initialization.
        # - Inject a logger.
        # - Refactor state modification (`filter_counts`) - maybe return status/reason codes.
        # - Move ENUMs nearby.
        # ... (Code as provided) ...


# From: PageviewProcessor class
# Current Location: src/wikipedia_pageviews/processor.py
# Target Location: processing/parser/pageview_parser.py (or pageview_handler.py)

    @staticmethod
    def classify_line_pattern(parts):
        """Classify the pattern of a line from pageview data."""
        # --- Analysis ---
        # Purpose: Simple helper to determine line format based on number of parts.
        # Logic: Basic if/elif based on len(parts).
        # Dependencies: None.
        # --- Refactoring Notes ---
        # - Useful helper for `extract_pageview_record`.
        # - As it's static, it can easily be moved.
        # --- Target Framework ---
        # - Move this static method along with `extract_pageview_record` to the new
        #   `PageviewParser` or `PageviewRecordHandler` class in `processing/parser/`.
        # ... (Code as provided) ...

    @staticmethod
    def analyze_page_name(page_name, page_prefixes, special_formats):
        """Analyze page names to identify prefixes and special formats."""
        # --- Analysis ---
        # Purpose: Helper to identify namespace prefixes (like "File:", "Special:") in page titles.
        # Logic: Splits on ':', checks for 'Special:'. Updates external Counter/defaultdict.
        # Dependencies: None (but modifies arguments passed by reference).
        # --- Refactoring Notes ---
        # - Helper for `extract_pageview_record`.
        # - Modifying external collections passed by reference can sometimes be less clear than returning results.
        #   Consider returning identified prefixes/formats instead.
        # --- Target Framework ---
        # - Move this static method along with `extract_pageview_record` to the new
        #   `PageviewParser` or `PageviewRecordHandler` class in `processing/parser/`.
        # - Consider refactoring to return values instead of modifying input collections.
        # ... (Code as provided) ...

    def extract_user_type_from_filename(self, filename: str) -> str:
        """ Parse the filename to identify and return the user type. """
        # --- Analysis ---
        # Purpose: Determines user type (user, spider, etc.) based on keywords in the filename.
        # Logic: Simple string checking on lowercased filename.
        # Dependencies: None (self is not used).
        # --- Refactoring Notes ---
        # - Doesn't use `self`, should be a static method or ideally a standalone function.
        # - This logic couples parsing results tightly to filename conventions. Is there a more robust way?
        #   Probably not easily available in the raw data, so this might be necessary.
        # --- Target Framework ---
        # - Move this function along with `extract_pageview_record` to the new
        #   `PageviewParser` or `PageviewRecordHandler` class/module in `processing/parser/`. Make it static or top-level.
        # ... (Code as provided) ...


# From: PageviewProcessor class
# Current Location: src/wikipedia_pageviews/processor.py
# Target Location: processing/parser/pageview_parser.py (or similar)

    def process_pageview_file_batches(
        self, file_path: str, wiki_filter: Optional[str] = None,
        batch_size: int = 100000, sample_limit: Optional[int] = None
    ) -> Generator[List[Dict[str, Any]], None, Dict[str, int]]:
        """ Stream and process pageview files with filtering and batching. """
        # --- Analysis ---
        # Purpose: Reads a compressed pageview file line by line, parses/filters each line using
        #          `extract_pageview_record`, batches valid records, and yields batches. Returns stats.
        # Logic: Handles file opening (gz/bz2), sets up filter set, iterates lines with tqdm,
        #        calls parser, manages batch list, handles sample limit, yields batches, logs summary.
        # Dependencies: os, bz2, gzip, tqdm, typing, self.get_timestamp_from_filename,
        #               self.extract_pageview_record, self.log, self.filter_counts.
        # --- Refactoring Notes ---
        # - This combines file reading/streaming with parsing orchestration and batching.
        # - File Reading/Streaming: Could be a separate utility or part of the parser class.
        # - Parsing Call: Should call the externalized parser function/method.
        # - Batching: This batching logic could live here (if this becomes part of the parser class)
        #   or potentially in the Orchestrator if batching is needed before sending to persistence.
        # - Stats/Logging: Should use injected logger. Stat collection could be returned or handled via callbacks.
        # - Dependency on `get_timestamp_from_filename` should be resolved (move utility).
        # --- Target Framework ---
        # - Move this logic primarily to the `PageviewParser` class in `processing/parser/`.
        # - The parser class's main method (e.g., `parse_file`) would perform this reading, parsing, and yielding of batches.
        # - It would call its own internal `extract_pageview_record`.
        # - It would receive `wiki_filter`, `batch_size`, `sample_limit` as arguments.
        # - It would use an injected logger.
        # ... (Code structure is reasonable, needs internal calls updated) ...


# From: WikipediaProcessor class
# Current Location: src/wikipedia_revisions/processor.py
# Target Location: processing/parser/stream_parser.py

    def stream_parse_xml(
        self, xml_stream: IO[bytes], # ... other args ...
    ) -> None:
        """ Stream and parse Wikipedia XML dump, extracting and storing revision tuples. """
        # --- Analysis ---
        # Purpose: The core LXML iterparse loop for processing the large XML dump. Identifies pages, revisions;
        #          extracts data; filters by namespace/redirects/minor edits; prepares and batches Mongo operations.
        # Logic: Complex loop using etree.iterparse. Manages state (current page, skipping). Calls helper methods
        #        (_extract_revision_data, _prepare_mongo_op, write_ops_batch_to_mongo, utilities). Handles stats and cleanup.
        # Dependencies: lxml (etree), time, datetime, os, pymongo (Collection, UpdateOne), psutil (optional), self (many methods), clear_element (utility).
        # --- Refactoring Notes ---
        # - This is the heart of the XML parsing logic. It *must* move out of the orchestrator/processor.
        # - It mixes parsing deeply with MongoDB operation preparation (`_prepare_mongo_op`) and batch writing (`write_ops_batch_to_mongo`).
        # - Parsing Logic: The iterparse loop, element handling (start/end page, revision), data extraction (`_extract_revision_data`),
        #   and filtering (namespace, redirect, minor) belong in the Parser component.
        # - Persistence Logic: Calls to `_prepare_mongo_op` and `write_ops_batch_to_mongo` should be removed.
        # - Data Flow: The parser should *yield* processed data (e.g., tuples or dictionaries representing `(page_hash, revision_data_dict)`)
        #   back to the caller (the Orchestrator).
        # - Batching: The Orchestrator will receive the yielded data and perform its own batching before sending batches to the Persistence service.
        # - Stats: The parser can collect parsing-specific stats and return them. Orchestrator aggregates overall stats.
        # - Element Clearing: Good use of `clear_element` utility for memory management.
        # --- Target Framework ---
        # - Move the core iterparse loop and element handling logic to `processing/parser/stream_parser.py` (likely a `parse` method).
        # - Move `_extract_revision_data` into the `StreamParser` as a helper.
        # - Remove calls to `_prepare_mongo_op` and `write_ops_batch_to_mongo`.
        # - Change the function to `yield` processed revision data (e.g., dicts or tuples) instead of preparing/writing Mongo ops.
        # - Inject logger and utilities.
        # ... (Code needs significant changes to decouple parsing from persistence) ...