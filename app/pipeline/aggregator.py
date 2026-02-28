"""
Aggregator module for merging structured outputs from multiple documents.

Combines extraction results into a unified dataset ready for CSV generation.
Designed to scale for AI extraction (multiple records per document).
"""
from dataclasses import dataclass
import uuid

from app.config import debug_log
from typing import Any


# Canonical schema for CSV and downstream systems
CANONICAL_COLUMNS = ("date", "vendor", "amount", "tax", "source", "record_id")


@dataclass
class AggregationResult:
    """Unified dataset. Keeps interface stable for future AI/analytics."""

    records: list[dict[str, Any]]
    total_amount: float
    total_tax: float
    document_count: int


def merge_data(extraction_results: list[dict | list[dict]]) -> list[dict[str, Any]]:
    """
    Merge structured outputs from multiple documents into a unified dataset.

    Supports:
    - Single record per document: [{"date": ..., "vendor": ..., ...}]
    - Multiple records per document (future AI): [doc1_records, doc2_records]
    where each item can be a dict or list[dict].

    Args:
        extraction_results: List of extraction outputs (each dict or list[dict])

    Returns:
        Flat list of normalized records ready for CSV generation
    """
    records = _flatten_records(extraction_results)
    # convert to flat list, then normalize each record, assigning a unique uuid-based record_id
    normalized = [_normalize_record(r) for r in records]
    debug_log("AGGREGATOR", f"Flattened {len(extraction_results)} doc(s) -> {len(normalized)} record(s)")
    total_amt = sum(_safe_float(r.get("amount", 0)) for r in normalized)
    debug_log("AGGREGATOR", f"Total amount: {total_amt}, records: {[r.get('vendor') for r in normalized[:5]]}{'...' if len(normalized) > 5 else ''}")
    return normalized


def merge_data_detailed(extraction_results: list[dict | list[dict]]) -> AggregationResult:
    """
    Merge with summary metadata. Use when totals or document count are needed.
    """
    records = merge_data(extraction_results)
    total_amount = sum(_safe_float(r.get("amount", 0)) for r in records)
    total_tax = sum(_safe_float(r.get("tax", 0)) for r in records)
    return AggregationResult(
        records=records,
        total_amount=total_amount,
        total_tax=total_tax,
        document_count=len(extraction_results),
    )


def _flatten_records(results: list[dict | list[dict]]) -> list[dict[str, Any]]:
    """Convert mixed single/multi-record per doc into flat list."""
    flat: list[dict[str, Any]] = []
    for item in results:
        if isinstance(item, dict):
            flat.append(item)
        elif isinstance(item, list):
            for rec in item:
                if isinstance(rec, dict):
                    flat.append(rec)
    return flat


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    """Ensure consistent schema and types for CSV and attach a unique ID.

    A UUID is used so that record identifiers remain unique even if datasets
    from multiple uploads are merged later. The previous implementation used a
    simple sequence number which reset per call, but the new approach is
    globally unique and safe for scalable, asynchronous processing.
    """
    # coerce missing or placeholder values to clean defaults
    raw_date = record.get("date", "")
    if raw_date is None or str(raw_date).strip().lower() in ("", "unknown", "n/a"):
        raw_date = ""
    raw_vendor = record.get("vendor", "")
    if raw_vendor is None or str(raw_vendor).strip().lower() in ("", "unknown", "n/a"):
        raw_vendor = ""

    normalized = {
        "date": str(raw_date),
        "vendor": str(raw_vendor),
        "amount": _safe_float(record.get("amount", 0)),
        "tax": _safe_float(record.get("tax", 0)),
        "source": str(record.get("source", "")),
        "record_id": str(uuid.uuid4()),
    }
    return normalized


def _safe_float(value: Any) -> float:
    """Convert to float; return 0.0 on failure."""
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
