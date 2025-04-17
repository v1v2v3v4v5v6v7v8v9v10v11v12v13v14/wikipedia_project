# From: PageviewProcessor class
# Current Location: src/wikipedia_pageviews/processor.py
# Target Location: utils/hashing.py or utils/identifiers.py

    def create_page_identifier(self, wiki_code: str, page_title: str) -> str:
        """ Generate a consistent hash-based identifier for a page. """
        # --- Analysis ---
        # Purpose: Creates a unique, URL-safe hash for a wiki page.
        # Logic: URL encodes title, combines with wiki code, SHA256 hashes, base64 encodes. Sound.
        # Dependencies: urllib.parse, hashlib, base64.
        # --- Refactoring Notes ---
        # - This is a pure utility function. It doesn't depend on the class state (`self`).
        # - It should be moved out of the class into a dedicated utility module.
        # - Consider making it a staticmethod if kept temporarily, but ideally move it out.
        # --- Target Framework ---
        # - Move this function to a new file like `utils/identifiers.py` or `utils/hashing.py`.
        encoded_title = urllib.parse.quote(page_title)
        hash_input = f"{wiki_code}:{encoded_title}"
        hash_obj = hashlib.sha256(hash_input.encode('utf-8'))
        return base64.urlsafe_b64encode(hash_obj.digest()).decode('utf-8').rstrip('=')[:22]


# From: WikipediaProcessor class
# Current Location: src/wikipedia_revisions/processor.py
# Target Location: utils/hashing.py (or utils/identifiers.py)

    def generate_hash_id(self, wiki_code, page_id, logger: logging.Logger) -> Optional[str]:
        # --- Analysis ---
        # Purpose: Wrapper around an imported `generate_hash_id` utility function.
        # Logic: Simply calls the imported function.
        # Dependencies: generate_hash_id (imported utility).
        # --- Refactoring Notes ---
        # - Same as `handle_processing_error` - this just delegates.
        # - The imported `generate_hash_id` should be placed in the target structure (e.g., `utils/hashing.py`).
        # - Components needing hash IDs (e.g., Parser, Orchestrator) should call the utility directly.
        # --- Target Framework ---
        # - Delete this wrapper method. Call the `generate_hash_id` utility directly. Ensure utility is placed correctly.
        return generate_hash_id(wiki_code, page_id, logger)


# Location: src/wikipedia_utils/common.py
# Target Location: utils/hashing.py (or utils/identifiers.py)

def generate_hash_id(wiki_code: str, page_id_mw: str, logger: logging.Logger) -> Optional[str]:
    """ Generate a stable SHA-256 hash for a wiki page entry. """
    # --- Analysis ---
    # Purpose: Creates a SHA256 hash from wiki code and page ID.
    # Logic: Checks for missing inputs, concatenates, encodes, hashes, returns hexdigest. Basic error handling.
    # Dependencies: hashlib, logging.
    # --- Refactoring Notes ---
    # - GOOD UTILITY: Pure function for generating an ID.
    # - Input Validation: Good check for missing inputs.
    # - Hashing Choice: SHA256 is standard.
    # - Comparison: Note this is different from the hash function inside the original `WikipediaDocument` which also included a timestamp. Ensure consistent usage or clear purpose for each hash type.
    # --- Target Framework ---
    # - Move this function to `utils/hashing.py` or `utils/identifiers.py`.
    # - Ensure components needing this specific hash call this utility.
    # ... (Code logic is good) ...