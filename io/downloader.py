
# From: WikipediaDownloader class
# Current Location: src/wikipedia_downloads/processor.py
# Target Location: downloads/downloader.py (or io/downloader.py)

def __init__(self, output_dir="~/WikipediaData", verbose=True):
    # --- Analysis ---
    # Purpose: Initializes the downloader with output path, verbosity, logger, and requests session.
    # Logic: Expands user path, calls internal logging setup, creates session.
    # Dependencies: os, logging, requests, self._setup_logging.
    # --- Refactoring Notes ---
    # - REMOVE call to self._setup_logging(). Logging setup should be external.
    # - The class should RECEIVE a logger instance via dependency injection.
    # - Keep output_dir and session initialization. Verbosity can be controlled by the logger level passed in.
    # --- Target Framework ---
    # - Move this method to the __init__ of the new Downloader class.
    # - Change signature: `__init__(self, output_dir, logger)`
    # - Remove `self.logger = self._setup_logging()`.
    # - Add `self.logger = logger`.
    # - `self.verbose` can likely be removed if logging levels are used properly.
    self.output_dir = os.path.expanduser(output_dir)
    self.verbose = verbose
    self.logger = self._setup_logging() # <<< REMOVE / REPLACE
    self.session = requests.Session()


# From: WikipediaDownloader class
# Current Location: src/wikipedia_downloads/processor.py
# Target Location: downloads/downloader.py (or io/downloader.py)

def _get_available_dates(self, base_url):
    # --- Analysis ---
    # Purpose: Finds available YYYYMMDD dump dates by scraping a Wikimedia index page.
    # Logic: Uses requests session and regex. Basic request error handling.
    # Dependencies: requests, re, self.session, self.logger.
    # --- Refactoring Notes ---
    # - This logic is specific to the Wikimedia dump structure and seems appropriate as a private
    #   helper within the Downloader class.
    # - Ensure `self.logger` refers to the injected logger instance in the refactored class.
    # - Could potentially add more specific error handling for parsing/regex failures.
    # --- Target Framework ---
    # - Keep this method within the refactored Downloader class.
    try:
        response = self.session.get(base_url, timeout=10)
        response.raise_for_status()
        dates = re.findall(r'href="(\d{8})/"', response.text )
        self.logger.info(f"Available dates for {base_url}: {dates}") # Use injected logger
        return dates
    except requests.RequestException as e:
        self.logger.error(f"Failed to fetch available dates from {base_url}: {str(e)}") # Use injected logger
        return []
    

# From: WikipediaDownloader class
# Current Location: src/wikipedia_downloads/processor.py
# Target Location: downloads/downloader.py (or io/downloader.py)

def _download_time_based_data(self, data_type, years, months, wiki_codes=None):
    """Handles both Pageviews and Clickstream (time-based data)"""
    # --- Analysis ---
    # Purpose: Orchestrates downloads for time-based datasets (pageviews, clickstream).
    # Logic: Iterates years/months, constructs URLs, fetches index pages, uses regex to find files,
    #        calculates output paths, calls internal _download_file helper. Tracks basic stats.
    # Dependencies: datetime, os, requests, re, self.session, self.logger, self.output_dir,
    #               get_wiki_output_path (external), self._download_file (to be replaced).
    # --- Refactoring Notes ---
    # - Replace calls to `self._download_file` with calls to the robust external `download_file` utility.
    # - Handle decompression by calling the external `decompress_file` utility *after* `download_file` succeeds.
    # - Ensure `self.logger` is the injected logger and pass it to utility functions.
    # - Ensure `get_wiki_output_path` is imported from its new location (e.g., io.path_utils).
    # - Regex for finding files might be brittle; consider alternatives if Wikimedia changes format.
    # - Error handling for index page fetching (`self.session.get`) could be more specific.
    # --- Target Framework ---
    # - Keep this method within the refactored Downloader class.
    # - Update internal calls to use external utilities for download/decompression.
    stats = {"attempted": 0, "success": 0}
    base_urls = { # Keep this config
        "pageviews": "https://dumps.wikimedia.org/other/pageview_complete/",
        "clickstream": "https://dumps.wikimedia.org/other/clickstream/"
    }

    for year in years:
        for month in months:
            ym = f"{year}-{month:02d}"
            # URL construction logic is fine
            url = f"{base_urls[data_type]}{ym[:4]}/{ym}/" if data_type == "pageviews" else f"{base_urls[data_type]}{ym}/"

            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()

                # Regex patterns seem okay for now
                if data_type == "pageviews":
                    pattern = r'href="(pageviews-\d{8}-[^"]+\.bz2)"'
                else: # clickstream
                    wikis = wiki_codes or ["enwiki"]
                    pattern = r'href="(clickstream-(?:{})-{}\.tsv\.gz)"'.format("|".join(wikis), ym)

                files = re.findall(pattern, response.text)
                self.logger.info(f"Found {len(files)} {data_type} files for {ym}") # Use injected logger
                self.logger.info(f"{files}") # Use injected logger

                for filename in files:
                    self.logger.info(f"Processing {filename}") # Use injected logger
                    # Call to external path utility (ensure imported correctly)
                    # Need to determine output dir/subdirs more clearly maybe
                    if data_type == "pageviews":
                        output_path_base = get_wiki_output_path(self.output_dir, data_type, ym, filename)
                    else:
                        output_path_base = get_wiki_output_path(self.output_dir, data_type, filename, wiki_codes) # Needs review

                    # Ensure directory exists (utility download function might also do this)
                    output_dir_for_file = os.path.dirname(output_path_base)
                    os.makedirs(output_dir_for_file, exist_ok=True)

                    file_url = f"{url}{filename}"
                    self.logger.info(f"Download URL: {file_url}") # Use injected logger
                    stats["attempted"] += 1

                    # --- REPLACE self._download_file ---
                    # Example replacement:
                    download_success = download_file( # Assuming download_file is imported
                        url=file_url,
                        output_dir=output_dir_for_file,
                        filename=filename,
                        logger=self.logger,
                        # max_retries=... # Add other params as needed
                    )
                    # --- END REPLACE ---

                    if download_success:
                        self.logger.info(f"Downloaded {filename} to {output_dir_for_file}") # Use injected logger
                        stats["success"] += 1
                        # --- ADD DECOMPRESSION CALL (if needed based on extension) ---
                        local_filepath = os.path.join(output_dir_for_file, filename)
                        if local_filepath.endswith(('.gz', '.bz2')):
                             decompressed_path = decompress_file( # Assuming decompress_file is imported
                                 filepath=local_filepath,
                                 logger=self.logger,
                                 delete_original=True # Example: Delete after decompress
                             )
                             if decompressed_path:
                                 self.logger.info(f"Decompressed to {decompressed_path}")
                             else:
                                 self.logger.error(f"Decompression failed for {local_filepath}")
                        # --- END ADD DECOMPRESSION ---
                    # else: The download_file utility should log errors

            except Exception as e: # Keep broad catch for now, but could be more specific
                self.logger.error(f"Failed {data_type} processing for {ym}: {str(e)}") # Use injected logger

    return stats



# From: WikipediaDownloader class
# Current Location: src/wikipedia_downloads/processor.py
# Target Location: downloads/downloader.py (or io/downloader.py)

def _download_wiki_based_data(self, dump_type, wiki_codes):
    """Handles Revisions/Pagelinks (wiki-based data)"""
    # --- Analysis ---
    # Purpose: Orchestrates downloads for wiki/date-based datasets (revisions, pagelinks).
    # Logic: Finds latest dump date, fetches index page, uses regex, calls internal _download_file.
    # Dependencies: Similar to _download_time_based_data: uses _get_available_dates, session, logger, regex,
    #               get_wiki_output_path, _download_file (to be replaced).
    # --- Refactoring Notes ---
    # - Same fundamental changes needed as for _download_time_based_data:
    #   - Replace `self._download_file` with calls to the robust external `download_file` utility.
    #   - Handle decompression *after* successful download by calling `decompress_file` utility.
    #   - Ensure `self.logger` is injected and passed to utilities.
    #   - Ensure `get_wiki_output_path` is imported from its new location.
    #   - Review complexity/brittleness of the file-finding regex.
    # --- Target Framework ---
    # - Keep this method within the refactored Downloader class.
    # - Update internal calls to use external utilities for download/decompression.
    stats = {"attempted": 0, "success": 0}

    for wiki in wiki_codes:
        base_url = f"https://dumps.wikimedia.org/{wiki}/"
        dates = self._get_available_dates(base_url) # Calls internal helper
        dump_date = max(dates) if dates else None
        self.logger.info(f"Latest available dump date for {wiki}: {dump_date}") # Use injected logger
        if not dump_date: # Changed from 'not dates' to use dump_date
            self.logger.warning(f"No dump dates found for {wiki}. Skipping.")
            continue

        dump_url = f"{base_url}{dump_date}/" # This seems correct

        try:
            response = self.session.get(dump_url, timeout=10)
            response.raise_for_status() # Good practice

            # Regex patterns defined here - okay, but verify robustness
            pattern = {
                "revisions": rf'href="/{wiki}/{dump_date}/({wiki}-{dump_date}-(?:stub-meta-history|stub-meta-current|stub-articles)\.xml\.gz)"',
                "pagelinks": rf'href="/{wiki}/{dump_date}/({wiki}-{dump_date}-pagelinks\.sql\.gz)"'
            }[dump_type]

            # Logger call for raw HTML might be too verbose, consider removing or setting to DEBUG
            # self.logger.info(f'{response.text}')

            # Simplify file extraction if regex guarantees capture group 1 is the filename
            # Assuming the pattern always has one capture group for the filename:
            files = re.findall(pattern, response.text)

            self.logger.info(f"Found {len(files)} files for {dump_type} on {dump_url}: {files}") # Use injected logger

            for filename in files:
                # Call external path utility
                output_path_base = get_wiki_output_path(self.output_dir, dump_type, filename, wiki_codes) # Needs review
                output_dir_for_file = os.path.dirname(output_path_base)
                os.makedirs(output_dir_for_file, exist_ok=True)

                # Construct file URL - ensure base_url vs dump_url usage is correct
                file_url = f"{base_url}{dump_date}/{filename}" # Looks correct
                self.logger.info(f"Attempting download: {filename} from {file_url}") # Use injected logger
                stats["attempted"] += 1

                # --- REPLACE self._download_file ---
                download_success = download_file( # Assuming download_file is imported
                    url=file_url,
                    output_dir=output_dir_for_file,
                    filename=filename,
                    logger=self.logger,
                    # max_retries=...
                )
                # --- END REPLACE ---

                if download_success:
                    self.logger.info(f"Downloaded {filename} to {output_dir_for_file}") # Use injected logger
                    stats["success"] += 1
                    # --- ADD DECOMPRESSION CALL ---
                    local_filepath = os.path.join(output_dir_for_file, filename)
                    if local_filepath.endswith(('.gz', '.bz2')):
                         decompressed_path = decompress_file( # Assuming utility imported
                             filepath=local_filepath,
                             logger=self.logger,
                             delete_original=True
                         )
                         if decompressed_path:
                             self.logger.info(f"Decompressed to {decompressed_path}")
                         else:
                             self.logger.error(f"Decompression failed for {local_filepath}")
                    # --- END DECOMPRESSION ---
                # else: download utility logs error

        except Exception as e: # Broad exception catch
            self.logger.error(f"Failed processing {dump_type} for {wiki} ({dump_date}): {str(e)}") # Use injected logger

    return stats


# From: WikipediaDownloader class
# Current Location: src/wikipedia_downloads/processor.py
# Target Location: downloads/downloader.py (or io/downloader.py)

def download(self, data_type, years=None, months=None, wiki_codes=None):
    """Unified download interface"""
    # --- Analysis ---
    # Purpose: Public method to dispatch to the correct internal download logic based on data type.
    # Logic: Simple if/elif/else branching. Sets default date/wiki values if none provided.
    # Dependencies: Internal _download_* methods, datetime.
    # --- Refactoring Notes ---
    # - This remains the primary public interface for the Downloader class.
    # - Logic is sound. Ensure the internal methods it calls are correctly refactored.
    # --- Target Framework ---
    # - Keep this method within the refactored Downloader class.
    if data_type in ["pageviews", "clickstream"]:
        # Use current year if years not specified
        years_to_process = years or [datetime.now().year]
        # Use all months if months not specified
        months_to_process = months or list(range(1, 13))
        return self._download_time_based_data(data_type, years_to_process,
                                           months_to_process, wiki_codes)
    elif data_type in ["revisions", "pagelinks"]:
        # Use enwiki if wiki_codes not specified
        wikis_to_process = wiki_codes or ["enwiki"]
        return self._download_wiki_based_data(data_type, wikis_to_process)
    else:
        self.logger.error(f"Unsupported data type requested: {data_type}") # Use injected logger
        raise ValueError(f"Unsupported data type: {data_type}")