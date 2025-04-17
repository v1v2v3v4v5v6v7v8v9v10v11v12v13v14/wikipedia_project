# From: PageviewProcessor class
# Current Location: src/wikipedia_pageviews/processor.py
# Target Location: Constants can be defined in relevant modules or a dedicated config/constants module.

    ACCESS_TYPE_ENUM = { ... }
    USER_TYPE_ENUM = { ... }
    # --- Analysis ---
    # Purpose: Defines mappings for access/user types to numerical values.
    # Logic: Simple dictionaries.
    # --- Refactoring Notes ---
    # - These are useful mappings.
    # - They could live as constants within the module that *uses* them most directly (e.g., the
    #   parsing/handler module that assigns these numeric types) or in a shared constants file
    #   (e.g., `processing/shared/constants.py` or `domain/constants.py`).
    # --- Target Framework ---
    # - Move these dictionaries out of the main class definition. Place in `processing/parser/pageview_handler.py`
    #   or `processing/shared/constants.py`.