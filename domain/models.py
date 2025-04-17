# From: WikipediaDocument class
# Current Location: src/wikipedia_documents/wikipedia_document.py
# Target Location: domain/models.py (or core/models.py)

def __post_init__(self):
    # --- Analysis ---
    # Purpose: Initializes default values (metadata, revisions) and generates hash_id if missing.
    # Logic: Sound. Correctly handles mutable defaults and calls internal hash generation.
    # Dependencies: Relies on self._generate_hash_id().
    # --- Refactoring Notes ---
    # - Keep this method largely as is within the relocated WikipediaDocument dataclass.
    # - Ensure _generate_hash_id is also moved with the class.
    # --- Target Framework ---
    # - This logic belongs with the WikipediaDocument dataclass wherever it ends up (e.g., domain/models.py).
    if not self.hash_id:
        self.hash_id = self._generate_hash_id()
    if self.metadata is None:
        self.metadata = {}
    if self.revisions is None:
        self.revisions = []

# From: WikipediaDocument class
# Current Location: src/wikipedia_documents/wikipedia_document.py
# Target Location: domain/models.py (or core/models.py)

def _generate_hash_id(self) -> str:
    # --- Analysis ---
    # Purpose: Creates a SHA256 hash based on wiki_code, doc_id, and timestamp. Used as a unique ID.
    # Logic: Correctly encodes inputs and generates hexdigest. Provides idempotency key based on these fields.
    # Dependencies: self.wiki_code, self.doc_id, self.timestamp.
    # --- Refactoring Notes ---
    # - Keep this method as a private helper within the relocated WikipediaDocument dataclass.
    # - Consider if content hashing is ever needed (outside scope of this function). For now, it's fine.
    # --- Target Framework ---
    # - This logic belongs with the WikipediaDocument dataclass (e.g., domain/models.py).
    return hashlib.sha256(
        f"{self.wiki_code}-{self.doc_id}-{self.timestamp.isoformat()}".encode()
    ).hexdigest()


# From: WikipediaDocument class
# Current Location: src/wikipedia_documents/wikipedia_document.py
# Target Location: domain/models.py (or core/models.py) - OR - persistence/providers/mongo/service.py

def to_mongo_dict(self) -> Dict:
    """Convert to MongoDB document format"""
    # --- Analysis ---
    # Purpose: Maps the dataclass fields to a dictionary suitable for MongoDB insertion.
    # Logic: Correctly structures the dictionary, uses hash_id as _id, renames revision fields, adds last_updated.
    # Dependencies: Relies on self attributes.
    # --- Refactoring Notes ---
    # - Option 1 (Keep with Domain): Keep this method within the relocated WikipediaDocument dataclass.
    #   This means the domain model has some awareness of one potential persistence format.
    # - Option 2 (Move to Persistence): Move this logic into the MongoPersistenceService. The service
    #   would take a WikipediaDocument object and know how to convert it into the specific dict needed
    #   for MongoDB. This makes the domain model completely persistence-agnostic.
    # - Option 2 is generally preferred for stricter separation but Option 1 is often acceptable.
    # --- Target Framework ---
    # - EITHER keep with WikipediaDocument (e.g., domain/models.py)
    # - OR move conversion logic to MongoPersistenceService (persistence/providers/mongo/service.py).
    return {
        "_id": self.hash_id,
        "doc_id": self.doc_id,
        "wiki_code": self.wiki_code,
        "title": self.title,
        "text": self.text,
        "timestamp": self.timestamp,
        "namespace": self.namespace,
        "is_redirect": self.is_redirect,
        "metadata": self.metadata,
        "revisions": [{"ts": rev[0], "eid": rev[1], "rev_id": rev[2]} for rev in self.revisions],
        "last_updated": datetime.datetime.utcnow()
    }