# wiki_utils/file_utils.py
"""Utilities for handling files and streams for Wikipedia data processing."""

import bz2
import gzip
import logging
from typing import Any, Optional, Union, BinaryIO, TextIO
from io import TextIOWrapper, BytesIO
import requests

logger = logging.getLogger(__name__)

def get_binary_stream(stream_or_path: Any, file_name: str) -> BinaryIO:
    """
    Get appropriate binary stream based on file type and compression.
    
    Args:
        stream_or_path: File-like object or path to file
        file_name: Name of the file for determining compression
        
    Returns:
        Binary stream suitable for reading
    """
    logger.debug(f"get_binary_stream called with stream_or_path type: {type(stream_or_path)}, file_name: {file_name}")
    should_close = False

    def wrap_compression(file_obj: BinaryIO, filename: str) -> BinaryIO:
        try:
            if filename.lower().endswith(".bz2"):
                logger.debug("File extension indicates bz2 compression, wrapping file object accordingly.")
                return bz2.BZ2File(file_obj, "rb")
            elif filename.lower().endswith(".gz"):
                logger.debug("File extension indicates gzip compression, wrapping file object accordingly.")
                return gzip.GzipFile(fileobj=file_obj, mode="rb")
            else:
                logger.debug("No compression detected based on file extension, returning raw file object.")
                return file_obj
        except Exception as e:
            logger.error(f"Error opening compressed stream: {str(e)}", exc_info=True)
            raise e

    # If it's already a binary stream, just return it
    if hasattr(stream_or_path, 'read') and not hasattr(stream_or_path, 'encoding'):
        logger.debug("Input is already a binary stream, returning as is.")
        return wrap_compression(stream_or_path, file_name)

    # If it's a string, handle URLs or open as file
    if isinstance(stream_or_path, str):
        if stream_or_path.startswith("http://") or stream_or_path.startswith("https://"):
            logger.debug(f"Input is a URL: {stream_or_path}, fetching content.")
            try:
                response = requests.get(stream_or_path, stream=True)
                response.raise_for_status()
                raw_stream = response.raw
                logger.debug("Successfully fetched URL content, wrapping stream for compression if needed.")
                return wrap_compression(raw_stream, file_name)
            except Exception as e:
                logger.error(f"Error fetching URL {stream_or_path}: {str(e)}", exc_info=True)
                raise e
        logger.debug(f"Input is a local file path: {stream_or_path}, opening file in binary mode.")
        file_obj = open(stream_or_path, "rb")
        should_close = True
        try:
            return wrap_compression(file_obj, file_name)
        except Exception:
            if should_close:
                try:
                    file_obj.close()
                except Exception:
                    pass
            raise

    # If it's a text stream, we need to get the underlying buffer
    if hasattr(stream_or_path, 'buffer'):
        logger.debug("Input is a text stream with buffer attribute, using underlying buffer.")
        file_obj = stream_or_path.buffer
        return wrap_compression(file_obj, file_name)

    # Assume it's already file-like but might not be binary
    logger.debug(f"Input is a file-like object of type {type(stream_or_path)}, using as is.")
    return wrap_compression(stream_or_path, file_name)

def get_text_stream(stream_or_path: Any, file_name: str, encoding: str = "utf-8") -> TextIO:
    """
    Get text stream from binary stream with proper encoding.
    
    Args:
        stream_or_path: File-like object or path to file
        file_name: Name of the file for determining compression
        encoding: Text encoding to use
        
    Returns:
        Text stream for reading
    """
    # If it's already a text stream, just return it
    if hasattr(stream_or_path, 'encoding'):
        return stream_or_path
    
    # Handle URLs directly
    if isinstance(stream_or_path, str) and (stream_or_path.startswith("http://") or stream_or_path.startswith("https://")):
        try:
            response = requests.get(stream_or_path, stream=True)
            response.raise_for_status()
            response.encoding = encoding
            return response.iter_lines(decode_unicode=True)
        except Exception as e:
            logger.error(f"Error fetching URL {stream_or_path}: {str(e)}", exc_info=True)
            raise e
    
    # Handle compressed files explicitly
    try:
        if isinstance(stream_or_path, str):
            if file_name.lower().endswith(".bz2"):
                binary = bz2.open(stream_or_path, mode="rt", encoding=encoding, errors="replace")
                return binary
            elif file_name.lower().endswith(".gz"):
                binary = gzip.open(stream_or_path, mode="rt", encoding=encoding, errors="replace")
                return binary
        else:
            if file_name.lower().endswith(".bz2"):
                binary = bz2.BZ2File(stream_or_path, "rb")
                return TextIOWrapper(binary, encoding=encoding, errors="replace")
            elif file_name.lower().endswith(".gz"):
                binary = gzip.GzipFile(fileobj=stream_or_path, mode="rb")
                return TextIOWrapper(binary, encoding=encoding, errors="replace")
    except Exception as e:
        logger.error(f"Error opening compressed text stream: {str(e)}", exc_info=True)
        raise e

    # Get binary stream first
    binary = get_binary_stream(stream_or_path, file_name)
    
    # Convert to text stream
    return TextIOWrapper(binary, encoding=encoding, errors="replace")

def is_binary_stream(stream: Any) -> bool:
    """
    Check if a stream is binary.
    
    Args:
        stream: Stream to check
        
    Returns:
        True if stream is binary, False otherwise
    """
    return hasattr(stream, 'read') and not hasattr(stream, 'encoding')

def safe_close(stream: Optional[Union[BinaryIO, TextIO]]) -> None:
    """
    Safely close a stream, catching and logging exceptions.
    
    Args:
        stream: Stream to close
    """
    if stream:
        try:
            stream.close()
        except Exception as e:
            logger.warning(f"Error closing stream: {e}")