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