from pathlib import Path
import gzip
import bz2
import io
from typing import Iterable, Union, TextIO

def open_stream(source: Union[str, Path, TextIO]) -> TextIO:
    """
    Open a text stream for the given source.
    - If `source` is a path ending in .gz, .bz2: decompress accordingly.
    - If it's a plain path: open as text.
    - If it's already a file-like TextIO: return as-is.
    """
    if isinstance(source, (str, Path)):
        fp = str(source)
        if fp.endswith('.gz'):
            return gzip.open(fp, 'rt')
        elif fp.endswith('.bz2'):
            return bz2.open(fp, 'rt')
        else:
            return open(fp, 'r')
    elif hasattr(source, 'read'):
        # Assume it's already a text-mode file-like
        return source
    else:
        raise TypeError(f"Unsupported source type: {type(source)}")

def open_streams(sources: Iterable[Union[str, Path, TextIO]]) -> Iterable[TextIO]:
    """
    Given an iterable of sources (paths or file-like), yield open TextIO streams.
    Caller is responsible for closing them when done.
    """
    for src in sources:
        yield open_stream(src)
