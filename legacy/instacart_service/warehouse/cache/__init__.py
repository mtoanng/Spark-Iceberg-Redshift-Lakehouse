"""In-process cache package (replaces Redis)."""
from .memory_cache import cached, clear_cache

__all__ = ["cached", "clear_cache"]
