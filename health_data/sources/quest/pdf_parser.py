"""Quest lab PDF parser utilities.

This module parses PDF bytes into pseudo-FHIR Observation dicts.
It mirrors the heuristics used in QuestAdapter._parse_pdf but operates on bytes.
"""
from __future__ import annotations
from typing import Iterable, Optional
import io, re

try:
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover
    pdfplumber = None

def parse_pdf_bytes(data: bytes, filename: str, patient_id: Optional[str]) -> Iterable[dict]:
    if not pdfplumber:
        raise RuntimeError('pdfplumber not installed; cannot parse PDF. Install dependency.')
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            for line in text.splitlines():
                if not line.strip():
                    continue
                cols = re.split(r'\s{2,}', line.strip())
                if len(cols) < 3:
                    continue
                test_name = cols[0]
                value_part = cols[1]
                ref_part = cols[-1]
                flag = None
                value_num = None
                unit = None
                m = re.match(r'([<>]?[0-9]+(?:\.[0-9]+)?)(?:\s*([A-Za-z/%µu]+))?(?:\s*(H|L|HI|LO|\*|\!))?', value_part)
                if m:
                    value_num_str, unit, flag = m.group(1), m.group(2), m.group(3)
                    try:
                        value_num = float(value_num_str.lstrip('<>'))
                    except Exception:
                        value_num = None
                ref_low = None; ref_high = None
                m2 = re.match(r'([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)', ref_part)
                if m2:
                    try:
                        ref_low = float(m2.group(1)); ref_high = float(m2.group(2))
                    except Exception:
                        pass
                obs_id = f"pdf:{filename}:{hash(line)}"
                pid = patient_id or 'self'
                yield {
                    'resourceType': 'Observation',
                    'id': obs_id,
                    'subject': {'reference': f'Patient/{pid}'},
                    'code': {'text': test_name, 'coding': []},
                    'effectiveDateTime': None,
                    'valueQuantity': {'value': value_num, 'unit': unit} if value_num is not None else None,
                    'valueString': None if value_num is not None else value_part,
                    'referenceRange': [{'low': {'value': ref_low} if ref_low is not None else None,
                                        'high': {'value': ref_high} if ref_high is not None else None}],
                    'interpretation': {'coding': [{'code': flag}]} if flag else None,
                    'raw_pdf_line': line
                }
