# Location: src/wikipedia_utils/common.py
# Target Location: io/downloader.py (or utils/downloader.py)

def download_file(url: str, output_dir: str, filename: str, logger: logging.Logger, max_retries: int = 3) -> bool:
    """ Download a file with progress, retries, size check, and robust error handling. """
    # --- Analysis ---
    # Purpose: Provides a robust, standalone function to download files. Handles retries, size checks, progress, errors.
    # Logic: Complex but comprehensive. Checks existence/size via HEAD, retry loop with exponential backoff, streams download, writes chunked, calculates progress/ETA, verifies final size, handles specific exceptions.
    # Dependencies: os, requests, time, datetime, timedelta, logging.
    # --- Refactoring Notes ---
    # - EXCELLENT UTILITY: This is the robust downloader implementation that should be used instead of the simpler/buggy versions seen inside other classes.
    # - Standalone: Well-designed to be independent, taking logger and params as arguments.
    # - Progress Bar: Manual progress bar implementation is good (avoids tqdm dependency if desired).
    # --- Target Framework ---
    # - Move this function to `io/downloader.py` or `utils/downloader.py`.
    # - Ensure all other components (Downloader classes in the original code) are refactored to *call this utility* instead of implementing their own download logic.
    # ... (Code logic is robust and well-suited as a central utility) ...


# Location: src/wikipedia_utils/common.py
# Target Location: io/path_utils.py (or utils/path_utils.py)

def get_wiki_output_path(wikicode: str, data_type: str, date: datetime,
                         processed: bool = False, file_format: str = None) -> Path:
    # --- Analysis ---
    # Purpose: Constructs a standardized, nested output file path based on wiki, type, date, and processing status. Ensures directory exists.
    # Logic: Uses `pathlib.Path` (good). Builds path components dynamically. Validates `file_format` based on `processed` flag. Creates directories. Returns Path object.
    # Dependencies: datetime, typing, pathlib.
    # --- Refactoring Notes ---
    # - GOOD UTILITY: Enforces a consistent directory structure for output files.
    # - Validation: Input validation for `file_format` is useful.
    # - Directory Creation: Automatically creates directories (`mkdir`).
    # - Flexibility: Parameters allow specifying raw vs processed paths.
    # --- Target Framework ---
    # - Move this function to `io/path_utils.py` or `utils/path_utils.py`.
    # - Components needing to generate output paths (e.g., Downloader, Orchestrator) will call this utility.
    # --- Potential Issue ---
    # The original implementation seems to take filename or other non-date arguments in some calls.
    # This revised signature ONLY uses date. This needs reconciliation. The original might need to be
    # split or adapted based on how paths are *actually* determined for different data types.
    # The version provided here assumes a date-based structure, which might not fit all original uses.
    # REVIEW AND ADJUST based on actual path requirements from the Downloader classes.

    # Base path construction needs careful review based on desired structure
    # This example assumes a structure like downloads/enwiki/pageviews/2023/10/processed/enwiki_pageviews_20231001.csv
    base_path = Path("wikipedia_project") / f"downloads/{wikicode}/{data_type}/{date.year}/{date.month:02d}" # Review this base path

    if processed:
        if file_format not in {"csv", "parquet", "xml"}: # Add/remove valid formats
            raise ValueError(f"Unsupported processed file format: {file_format}.")
        base_path = base_path / "processed"
        output_file = base_path / f"{wikicode}_{data_type}_{date:%Y%m%d}.{file_format}"
    else:
        # Assuming raw files also follow a date pattern primarily
        # Needs adjustment if raw filenames vary significantly (like including timestamps etc.)
        # The original function signature in other files might need to pass the actual source filename
        # for raw storage, rather than constructing it purely from date.
        if file_format not in {"bz2", "gzip", "gz", "tsv", "xml"}: # Added gz
             raise ValueError(f"Unsupported raw file format: {file_format}.")
        base_path = base_path / "raw"
        # This filename generation might be too simplistic for raw files.
        # It might be better to pass the original downloaded filename here.
        output_file = base_path / f"{wikicode}_{data_type}_{date:%Y%m%d}.{file_format}" # REVIEW THIS FILENAME PART

    # Ensure the directory exists
    base_path.mkdir(parents=True, exist_ok=True)
    return output_file