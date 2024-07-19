from __future__ import annotations  # can be removed at 3.8 EOL

__version__: str | None
__version_tuple__: tuple | None

try:
    # This file is auto-generated, and could be missing.
    # Tell mypy to ignore it
    from ._version import __version__, __version_tuple__  # type: ignore
except ImportError:
    __version__ = None
    __version_tuple__ = None

__all__ = (
    "__version__",
    "__version_tuple__",
)

from .core import SearchEngine
