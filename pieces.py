import os
from pathlib import Path
from typing import List, Optional, Dict

# From downloader
# Public Interface
def download(
    self,
    data_type: str,
    years: Optional[List[int]] = None,
    months: Optional[List[int]] = None,
    wiki_codes: Optional[List[str]] = None,
) -> Dict[str, int]:
    """
    Unified download interface for all supported data types.

    Args:
        data_type: One of ['pageviews', 'clickstream', 'revisions', 'pagelinks']
        years: List of years to download (time-based data only)
        months: List of months to download (1-12, time-based data only)
        wiki_codes: List of wiki project codes (e.g., ['enwiki', 'dewiki'])

    Returns:
        Dictionary with download statistics {'attempted': X, 'success': Y}
    """
    if data_type in ["pageviews", "clickstream"]:
        return self._download_time_based_data(
            data_type,
            years or [datetime.now().year],
            months or list(range(1, 13)),
            wiki_codes,
        )
    elif data_type in ["revisions", "pagelinks"]:
        self.logger.info(f"Wiki codes: {wiki_codes}")
        return self._download_wiki_based_data(data_type, wiki_codes)
    raise ValueError(f"Unsupported data type: {data_type}")

def download_file(
    self,
    url: str,
    output_path: Path,
    max_retries: int = 3,
    chunk_size: int = 8192,
    timeout: int = 30,
    min_expected_bytes: Optional[int] = None,
) -> bool:
    """
    Robust file downloader with retries, progress tracking, size verification, and error handling.
    """
    if output_path.exists():
        self.logger.info(f"File already exists, skipping download: {output_path}")
        return True

    temp_path = output_path.with_suffix(".tmp")

    for attempt in range(max_retries):
        try:
            head = self.session.head(url, timeout=timeout, allow_redirects=True)
            head.raise_for_status()

            remote_size = int(head.headers.get("Content-Length", 0))
            if min_expected_bytes and remote_size < min_expected_bytes:
                raise ValueError(
                    f"Remote file too small ({remote_size} < {min_expected_bytes} bytes)"
                )

            if output_path.exists():
                local_size = output_path.stat().st_size
                if local_size == remote_size:
                    self.logger.info(f"File exists with correct size: {output_path}")
                    return True

            start_time = datetime.now()
            with self.session.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))
                with open(temp_path, "wb") as f, tqdm(
                    total=total, unit='B', unit_scale=True, unit_divisor=1024,
                    desc=f"Downloading {output_path.name}", initial=0
                ) as bar:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))

            if remote_size > 0 and temp_path.stat().st_size != remote_size:
                raise IOError(
                    f"Size mismatch: {temp_path.stat().st_size} != {remote_size}"
                )

            temp_path.rename(output_path)
            self.logger.info(f"Successfully downloaded: {output_path}")
            return True

        except Exception as e:
            if attempt == max_retries - 1:
                self.logger.error(f"Download failed after {max_retries} attempts: {str(e)}")
                if temp_path.exists():
                    temp_path.unlink()
                return False

            wait_time = min(2**attempt, 60)
            self.logger.warning(f"Attempt {attempt + 1} failed. Retrying in {wait_time}s...")
            time.sleep(wait_time)

    return False


def _process_file(
    self,
    data_type: str,
    identifier: str,
    filename: str,
    year_month: Optional[str] = None,
    dump_date: Optional[str] = None,
    wiki_codes: Optional[List[str]] = None,
) -> bool:
    """Handle single file download and processing"""
    # Build appropriate URL based on data type
    if data_type in ["revisions", "pagelinks"]:
        file_url = f"{self.BASE_URLS[data_type].format(wiki_code=identifier)}{dump_date}/{filename}"
        self.logger.info(f"Downloading {filename} from {file_url}")
    else:
        file_url = f"{identifier}{filename}"
        self.logger.info(f"Downloading {filename} from {file_url}")

    # Determine output path and convert to Path object
    wiki_code = next((code for code in wiki_codes if code in filename), None) if wiki_codes else None
    output_path = Path(self._get_output_path(data_type, filename, year_month, dump_date, wiki_code))
    os.makedirs(output_path.parent, exist_ok=True)

    # Download file
    if not self.download_file(file_url, output_path):
        return False
    
    # Handle decompression if needed
    if output_path.suffix in (".gz", ".bz2"):
        return decompress_file(output_path, logger=self.logger)
    return True

