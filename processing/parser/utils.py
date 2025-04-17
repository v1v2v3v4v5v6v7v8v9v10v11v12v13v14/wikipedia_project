# Location: src/wikipedia_utils/common.py
# Target Location: processing/parser/xml_utils.py (or similar within parser module)

def clear_element(elem: Optional[Any], logger: logging.Logger) -> None:
    """ Safely clear an lxml element to free memory during iterative parsing. """
    # --- Analysis ---
    # Purpose: Crucial memory management technique for `lxml.etree.iterparse`. Clears element content and breaks parent links.
    # Logic: Checks for lxml availability and correct type, calls `elem.clear()`, removes preceding siblings to break parent reference. Includes robust error handling.
    # Dependencies: lxml (etree), logging, LXML_AVAILABLE_FOR_ERROR.
    # --- Refactoring Notes ---
    # - ESSENTIAL UTILITY for large XML parsing with lxml.
    # - Logic is standard practice for iterparse memory management.
    # - Error handling is good, logs critical if cleanup fails.
    # --- Target Framework ---
    # - This function is tightly coupled to the XML parsing process using lxml.
    # - Move it to a utility module *within* the parsing subsystem, e.g., `processing/parser/xml_utils.py`.
    # - The `StreamParser` using lxml iterparse will call this utility.
    # ... (Code logic is good and important) ...