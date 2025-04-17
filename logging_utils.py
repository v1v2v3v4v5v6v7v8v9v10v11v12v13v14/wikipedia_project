# Location: src/wikipedia_utils/common.py
# Target Location: config/logging_setup.py OR utils/logging_utils.py (or similar top-level)

def setup_logger(log_dir: str, debug: bool, verbose: bool, logger_name: str = "wiki_processor") -> logging.Logger:
    """ Configure and return a logger instance. """
    # --- Analysis ---
    # Purpose: Centralized function to create and configure a logger with console and rotating file handlers.
    # Logic: Gets logger instance, clears existing handlers (important!), sets level based on flags,
    #        creates formatter, adds console handler, creates log directory, adds file handler.
    # Dependencies: logging, logging.handlers, os, sys.
    # --- Refactoring Notes ---
    # - GOOD UTILITY: This is a well-structured function for setting up logging. It avoids `basicConfig`.
    # - SINGLE CALL: This function should be called *only once* at the very start of the application
    #   (in the main entry point script, e.g., `scripts/run_wiki_processing.py`) to configure logging for the entire run.
    # - RETURN VALUE: It correctly returns the configured logger instance.
    # --- Target Framework ---
    # - Move this function to a dedicated logging setup module, e.g., `config/logging_setup.py` or `utils/logging_utils.py`.
    # - Call it ONCE from the main script (`scripts/run_...`).
    # - Pass the returned logger instance via dependency injection to classes that need it (Orchestrator, Parser, Persistence, etc.).
    # ... (Code logic is good) ...


    # Location: src/wikipedia_utils/common.py
# Target Location: utils/error_handling.py (or processing/shared/error_handler.py)

def handle_error(error: Exception, stats: dict, logger: logging.Logger, debug: bool = False) -> dict:
    """ Handle processing errors, log them, and update a stats dictionary. """
    # --- Analysis ---
    # Purpose: Standardizes how exceptions are processed - updates stats dict, logs error details (incl. traceback if debug).
    # Logic: Initializes stats keys if missing, increments error count, formats error details, adds traceback/XML info conditionally, logs, returns updated stats.
    # Dependencies: datetime, timezone, traceback, logging, etree (optional for XML details), LXML_AVAILABLE_FOR_ERROR.
    # --- Refactoring Notes ---
    # - GOOD UTILITY: Provides consistent error reporting structure.
    # - XML Specifics: The check for `etree.XMLSyntaxError` ties it slightly to XML processing. If used *only* there, it could go in `processing/shared/`. If used more broadly, it belongs in `utils/`.
    # - `LXML_AVAILABLE_FOR_ERROR`: This global flag pattern is okay but less explicit than passing a flag or checking within the function if needed.
    # --- Target Framework ---
    # - Move this function to `utils/error_handling.py` (if potentially reusable beyond processing) or `processing/shared/error_handler.py` (if only for processing errors).
    # - Components like the Orchestrator would call this utility when catching exceptions.
    # ... (Code logic is good) ...