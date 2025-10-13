"""Base adapter interfaces for data sources."""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable, Any, Dict, Optional, Sequence
from dataclasses import dataclass
from datetime import datetime

@dataclass
class IngestResult:
    resource: str
    records_fetched: int
    records_loaded: int
    started_at: datetime
    finished_at: datetime
    status: str
    error: Optional[str] = None

class SourceAdapter(ABC):
    source_system: str

    @abstractmethod
    def authenticate(self) -> None:
        """Obtain or refresh credentials/tokens. Should be idempotent."""

    @abstractmethod
    def list_resources(self) -> Sequence[str]:
        """Return supported resource names."""

    @abstractmethod
    def fetch(self, resource: str, since: Optional[str] = None, until: Optional[str] = None) -> Iterable[dict]:
        """Yield raw resource records (as dicts)."""

    @abstractmethod
    def load_raw(self, resource: str, record: dict) -> None:
        """Persist raw record into source-specific raw or existing tables."""

    def transform_and_load_canonical(self, resource: str, record: dict) -> None:  # optional override
        """Optional: map raw record into canonical tables."""
        return

    def ingest(self, resources: Sequence[str], since: Optional[str], until: Optional[str], canonical: bool = False) -> Iterable[IngestResult]:
        from datetime import datetime
        for res in resources:
            start = datetime.utcnow()
            fetched = 0
            loaded = 0
            status = 'success'
            err = None
            try:
                for rec in self.fetch(res, since=since, until=until):
                    fetched += 1
                    self.load_raw(res, rec)
                    loaded += 1
                    if canonical:
                        self.transform_and_load_canonical(res, rec)
            except Exception as e:  # noqa: BLE001
                status = 'error'
                err = str(e)
            finish = datetime.utcnow()
            yield IngestResult(resource=res, records_fetched=fetched, records_loaded=loaded, started_at=start, finished_at=finish, status=status, error=err)
