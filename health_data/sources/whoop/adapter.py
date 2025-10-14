"""WHOOP adapter built atop existing ingestion logic.

This adapter reuses existing functions in whoop_ingest.py and db.py without yet
migrating raw storage into separate raw tables (transitional phase).
"""
from __future__ import annotations
from typing import Iterable, Optional, Sequence
from . import __doc__  # noqa: F401
from health_data.sources.base.adapter import SourceAdapter
from health_data.db.unified import transform_record
from .auth import get_access_token, TOKEN_MANAGER
from .resources import RESOURCE_MAP
from .storage import store_record

class WhoopAdapter(SourceAdapter):
    source_system = 'whoop'

    def authenticate(self) -> None:
        get_access_token()

    def list_resources(self) -> Sequence[str]:
        return list(RESOURCE_MAP.keys())

    def fetch(self, resource: str, since: Optional[str] = None, until: Optional[str] = None) -> Iterable[dict]:
        fetcher = RESOURCE_MAP[resource]
        if resource in {'profile', 'body'}:
            yield fetcher()
        else:
            yield from fetcher(start=since, end=until)

    def load_raw(self, resource: str, record: dict) -> None:
        # Persist to raw whoop tables
        store_record(resource, record)

    def transform_and_load_unified(self, resource: str, record: dict) -> None:  # override
        # Delegate to unified transform dispatcher
        transform_record(resource, record)
