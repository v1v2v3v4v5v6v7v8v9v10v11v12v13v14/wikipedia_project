from pathlib import Path
from datetime import datetime
from typing import Optional, Union
from config.settings import OUTPUT_DIRECTORY_PATH

def get_wiki_output_path(
    base_dir: Path,
    wiki_code: str,
    data_type: str,
    *,
    date: Optional[datetime] = None,
    filename: Optional[str] = OUTPUT_DIRECTORY_PATH,
    processed: bool = False,
    file_format: Optional[str] = None
) -> Path:
    """
    Generates standardized output paths with flexible configuration.
    
    Args:
        base_dir: Root directory for all downloads
        wiki_code: Wiki project code (e.g., 'enwiki')
        data_type: Data type ('pageviews', 'revisions', etc.)
        date: Date for time-based organization (optional)
        filename: Original filename if preserving (optional)
        processed: Whether this is processed output
        file_format: File extension (auto-detected if None)
    
    Returns:
        Complete Path object with parent directories created
    """
    # Validate inputs
    if not (date or filename):
        raise ValueError("Either date or filename must be provided")
    
    # Build base path components
    path_parts = [base_dir, wiki_code, data_type]
    
    # Add date-based subdirectories if available
    if date:
        path_parts.extend([str(date.year), f"{date.month:02d}"])
    
    # Add processing stage
    path_parts.append("processed" if processed else "raw")
    
    # Determine final filename
    if filename:
        final_name = filename
        if file_format and not filename.endswith(f".{file_format}"):
            final_name = f"{Path(filename).stem}.{file_format}"
    elif date:
        final_name = f"{wiki_code}_{data_type}_{date:%Y%m%d}.{file_format or 'data'}"
    else:
        raise ValueError("Cannot generate filename without date or source filename")
    
    # Construct full path
    output_path = Path(*path_parts) / final_name
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    return output_path