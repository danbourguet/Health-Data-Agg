"""Quest file-based adapter (PDF / JSON / NDJSON).

Parsing strategy:
 - If JSON / NDJSON -> treat as FHIR resources directly (Patient / Observation).
 - If PDF -> use pdfplumber (added dependency) to extract tabular lab results and map to pseudo-FHIR Observation dicts.

PDF heuristic (typical Quest lab PDF): lines with pattern:
  TEST NAME <whitespace> VALUE <whitespace> (FLAG?) <whitespace> REFERENCE RANGE
We attempt to parse columns by splitting on two+ spaces and applying simple regex.

Limitations: This is a best-effort parser; complex formatting or multi-line reference ranges may need refinement.
"""
from __future__ import annotations
from typing import Iterable, Sequence, Optional
from pathlib import Path
import json, re
from health_data.sources.base.adapter import SourceAdapter
from db import upsert_quest_patient, upsert_quest_observation

try:
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover
    pdfplumber = None

class QuestAdapter(SourceAdapter):
    source_system = 'quest'
    _resources = ['patient', 'observations']

    def __init__(self, path_: str, patient_id: Optional[str]):
        self.path = Path(path_)
        if not self.path.exists():
            raise FileNotFoundError(f'Path not found: {self.path}')
        self.patient_id_override = patient_id

    def authenticate(self) -> None:  # no auth required
        return

    def list_resources(self) -> Sequence[str]:
        return self._resources

    # ---- File iteration helpers ----

    def _iter_files(self) -> Iterable[Path]:
        if self.path.is_file():
            yield self.path
        else:
            for ext in ('*.json', '*.ndjson', '*.pdf'):
                yield from self.path.glob(ext)

    def _iter_json_objects(self, p: Path):
        txt = p.read_text(encoding='utf-8')
        try:
            data = json.loads(txt)
            if isinstance(data, list):
                for obj in data:
                    if isinstance(obj, dict):
                        yield obj
            elif isinstance(data, dict):
                yield data
        except json.JSONDecodeError:
            for line in txt.splitlines():
                line=line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        yield obj
                except Exception:
                    continue

    def _parse_pdf(self, p: Path) -> Iterable[dict]:
        if not pdfplumber:
            raise RuntimeError('pdfplumber not installed; cannot parse PDF. Install dependency.')
        with pdfplumber.open(p) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                for line in text.splitlines():
                    # Simple heuristic skip headers
                    if not line.strip():
                        continue
                    # Split on 2+ spaces
                    cols = re.split(r'\s{2,}', line.strip())
                    if len(cols) < 3:
                        continue
                    # Attempt mapping: TEST, VALUE(+FLAG), REF RANGE (last two columns maybe)
                    test_name = cols[0]
                    value_part = cols[1]
                    ref_part = cols[-1]
                    flag = None
                    value_num = None
                    unit = None
                    # Extract value + unit (number + optional unit)
                    m = re.match(r'([<>]?[0-9]+(?:\.[0-9]+)?)(?:\s*([A-Za-z/%µu]+))?(?:\s*(H|L|HI|LO|\*|\!))?', value_part)
                    if m:
                        value_num_str, unit, flag = m.group(1), m.group(2), m.group(3)
                        try:
                            value_num = float(value_num_str.lstrip('<>'))
                        except Exception:
                            value_num = None
                    # Reference range low-high
                    ref_low = None; ref_high = None
                    m2 = re.match(r'([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)', ref_part)
                    if m2:
                        try:
                            ref_low = float(m2.group(1)); ref_high = float(m2.group(2))
                        except Exception:
                            pass
                    obs_id = f"pdf:{p.name}:{hash(line)}"
                    patient_id = self.patient_id_override or 'self'
                    # Build pseudo-FHIR Observation
                    yield {
                        'resourceType': 'Observation',
                        'id': obs_id,
                        'subject': {'reference': f'Patient/{patient_id}'},
                        'code': {'text': test_name, 'coding': []},
                        'effectiveDateTime': None,  # Could later parse from PDF context (date on first page)
                        'valueQuantity': {'value': value_num, 'unit': unit} if value_num is not None else None,
                        'valueString': None if value_num is not None else value_part,
                        'referenceRange': [{'low': {'value': ref_low} if ref_low is not None else None,
                                            'high': {'value': ref_high} if ref_high is not None else None}],
                        'interpretation': {'coding': [{'code': flag}]} if flag else None,
                        'raw_pdf_line': line
                    }

    # ---- Adapter core ----

    def fetch(self, resource: str, since: Optional[str] = None, until: Optional[str] = None) -> Iterable[dict]:
        for file in self._iter_files():
            if file.suffix.lower() in ('.json', '.ndjson'):
                for obj in self._iter_json_objects(file):
                    rtype = obj.get('resourceType')
                    if resource == 'patient' and rtype == 'Patient':
                        yield obj
                    elif resource == 'observations' and rtype == 'Observation':
                        dt = obj.get('effectiveDateTime') or obj.get('issued')
                        if since and dt and dt < since:
                            continue
                        if until and dt and dt >= until:
                            continue
                        yield obj
            elif file.suffix.lower() == '.pdf' and resource == 'observations':
                yield from self._parse_pdf(file)

    def load_raw(self, resource: str, record: dict) -> None:
        if resource == 'patient':
            upsert_quest_patient(record)
        elif resource == 'observations':
            upsert_quest_observation(record)

    def transform_and_load_canonical(self, resource: str, record: dict) -> None:
        if resource == 'observations':
            from health_data.db.canonical import transform_quest_observation, get_conn
            with get_conn() as conn:
                transform_quest_observation(conn, record)
                conn.commit()

__all__ = ['QuestAdapter']
