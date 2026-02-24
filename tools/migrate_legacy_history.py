"""
Import legacy historical snapshots into current Postgres schema.

Sources supported:
- output/archive/**/combined_results*.json
- output/archive/**/combined_results*.csv
- optional legacy sqlite file (data/flights.db)
"""

from __future__ import annotations

import argparse
import ast
import csv
import datetime as dt
import json
import re
import sqlite3
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import bulk_insert_offers, bulk_insert_raw_meta, get_session, init_db
from models.flight_offer import FlightOfferORM


def parse_args():
    p = argparse.ArgumentParser(description="Migrate legacy flight history into current DB")
    p.add_argument("--archive-root", default="output/archive")
    p.add_argument("--sqlite-path", default="data/flights.db")
    p.add_argument("--limit-files", type=int, default=0, help="0 means no limit")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--include-csv", action="store_true", help="Include CSV sources even when JSON exists")
    return p.parse_args()


def _first(row: Dict[str, Any], keys: List[str]):
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return None


def _to_float(v):
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v):
    if v in (None, "", "None"):
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def _to_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"true", "1", "y", "yes"}:
        return True
    if s in {"false", "0", "n", "no"}:
        return False
    return None


def _to_datetime(v):
    if v in (None, "", "None"):
        return None
    if isinstance(v, dt.datetime):
        return v.replace(tzinfo=None)
    s = str(v).strip()
    for candidate in (s, s.replace("Z", "+00:00")):
        try:
            t = dt.datetime.fromisoformat(candidate)
            return t.replace(tzinfo=None)
        except Exception:
            continue
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def _parse_raw_offer(v):
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if not isinstance(v, str):
        return {"value": str(v)}
    s = v.strip()
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        pass
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, dict):
            return parsed
        return {"value": parsed}
    except Exception:
        return {"_raw_offer_text": s[:4000]}


def _parse_source_timestamp(path: Path) -> dt.datetime:
    name = path.name
    m = re.search(r"combined_results_(\d{8}T\d{6})Z", name)
    if m:
        try:
            return dt.datetime.strptime(m.group(1), "%Y%m%dT%H%M%S")
        except Exception:
            pass
    parent = path.parent.name
    m2 = re.search(r"batch_(\d{8})_(\d{6})", parent)
    if m2:
        try:
            return dt.datetime.strptime(m2.group(1) + m2.group(2), "%Y%m%d%H%M%S")
        except Exception:
            pass
    return dt.datetime.fromtimestamp(path.stat().st_mtime)


def _iter_json_rows(path: Path) -> Iterable[Dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    return [r for r in payload if isinstance(r, dict)]


def _iter_csv_rows(path: Path) -> Iterable[Dict[str, Any]]:
    rows = []
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                rows.append(dict(r))
    except Exception:
        return []
    return rows


def _build_core_and_meta(
    row: Dict[str, Any],
    scrape_id,
    scraped_at: dt.datetime,
    source_endpoint: str,
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], Tuple]]:
    airline = _first(row, ["airline", "airlineCode"])
    flight_number = _first(row, ["flight_number", "flightNumber", "operatingFlightNumber"])
    origin = _first(row, ["origin", "task_origin"])
    destination = _first(row, ["destination", "task_destination"])
    departure = _to_datetime(_first(row, ["departure"]))
    cabin = _first(row, ["cabin", "cabinClass", "task_cabin"])
    brand = _first(row, ["brand", "brandId"])
    fare_basis = _first(row, ["fare_basis", "fareBasis", "bookingClass"])
    price_total = _to_float(_first(row, ["price_total_bdt", "total_amount", "total", "price"]))
    seat_available = _to_int(_first(row, ["seat_available", "seatsRemaining", "seats_remaining_count", "seatsRemaining_count"]))
    seat_capacity = _to_int(_first(row, ["seat_capacity"]))

    if not airline or not flight_number or not origin or not destination or departure is None:
        return None
    if price_total is None:
        return None

    airline = str(airline).upper()
    origin = str(origin).upper()
    destination = str(destination).upper()
    flight_number = str(flight_number)

    core = {
        "scrape_id": scrape_id,
        "scraped_at": scraped_at,
        "airline": airline,
        "flight_number": flight_number,
        "origin": origin,
        "destination": destination,
        "departure": departure,
        "cabin": str(cabin) if cabin is not None else None,
        "brand": str(brand) if brand is not None else None,
        "price_total_bdt": price_total,
        "fare_basis": str(fare_basis) if fare_basis is not None else None,
        "seat_capacity": seat_capacity,
        "seat_available": seat_available,
    }

    raw_offer = _parse_raw_offer(_first(row, ["raw_offer"]))
    soldout = _to_bool(_first(row, ["soldout"]))
    meta = {
        "currency": _first(row, ["currency"]),
        "fare_amount": _to_float(_first(row, ["fare_amount", "fare"])),
        "tax_amount": _to_float(_first(row, ["tax_amount", "taxes", "taxes_amount"])),
        "baggage": _first(row, ["baggage"]),
        "aircraft": _first(row, ["aircraft", "equipment", "aircraft_type"]),
        "equipment_code": _first(row, ["equipment_code"]),
        "duration_min": _to_int(_first(row, ["duration_min", "duration"])),
        "stops": _to_int(_first(row, ["stops"])),
        "arrival": _to_datetime(_first(row, ["arrival"])),
        "estimated_load_factor_pct": _to_float(_first(row, ["estimated_load_factor_pct"])),
        "inventory_confidence": "reported" if seat_available is not None else "unknown",
        "booking_class": _first(row, ["booking_class", "bookingClass"]),
        "soldout": soldout,
        "adt_count": _to_int(_first(row, ["adt_count"])),
        "chd_count": _to_int(_first(row, ["chd_count"])),
        "inf_count": _to_int(_first(row, ["inf_count"])),
        "fare_ref_num": _first(row, ["fare_ref_num"]),
        "fare_search_reference": _first(row, ["fare_search_reference"]),
        "source_endpoint": source_endpoint,
        "raw_offer": raw_offer,
        "scraped_at": scraped_at,
    }

    identity_key = (
        scrape_id,
        airline,
        origin,
        destination,
        departure,
        flight_number,
        core["cabin"],
        core["fare_basis"],
        core["brand"],
    )
    return core, meta, identity_key


def _query_offer_ids_by_scrape(scrape_id) -> Dict[Tuple, int]:
    s = get_session()
    try:
        rows = (
            s.query(FlightOfferORM)
            .filter(FlightOfferORM.scrape_id == scrape_id)
            .all()
        )
        mapping = {}
        for r in rows:
            key = (
                scrape_id,
                r.airline,
                r.origin,
                r.destination,
                r.departure,
                r.flight_number,
                r.cabin,
                r.fare_basis,
                r.brand,
            )
            mapping[key] = r.id
        return mapping
    finally:
        s.close()


def _process_rows(rows: Iterable[Dict[str, Any]], source_label: str, scraped_at: dt.datetime, dry_run: bool):
    scrape_id = uuid.uuid5(uuid.NAMESPACE_URL, f"legacy:{source_label}:{scraped_at.isoformat()}")
    core_rows = []
    meta_rows = []
    seen = set()
    skipped = 0
    for row in rows:
        built = _build_core_and_meta(
            row=row,
            scrape_id=scrape_id,
            scraped_at=scraped_at,
            source_endpoint=source_label,
        )
        if not built:
            skipped += 1
            continue
        core, meta, identity_key = built
        if identity_key in seen:
            continue
        seen.add(identity_key)
        core_rows.append(core)
        meta_rows.append((identity_key, meta))

    if dry_run:
        return {
            "scrape_id": str(scrape_id),
            "core_candidates": len(core_rows),
            "meta_candidates": len(meta_rows),
            "skipped": skipped,
            "inserted_core": 0,
            "inserted_meta": 0,
        }

    inserted_core = bulk_insert_offers(core_rows) if core_rows else 0
    id_map = _query_offer_ids_by_scrape(scrape_id=scrape_id) if core_rows else {}
    to_insert_meta = []
    for identity_key, meta in meta_rows:
        offer_id = id_map.get(identity_key)
        if not offer_id:
            continue
        meta_item = dict(meta)
        meta_item["flight_offer_id"] = offer_id
        to_insert_meta.append(meta_item)
    inserted_meta = bulk_insert_raw_meta(to_insert_meta) if to_insert_meta else 0
    return {
        "scrape_id": str(scrape_id),
        "core_candidates": len(core_rows),
        "meta_candidates": len(meta_rows),
        "skipped": skipped,
        "inserted_core": int(inserted_core or 0),
        "inserted_meta": int(inserted_meta or 0),
    }


def _collect_archive_files(root: Path, include_csv: bool) -> List[Path]:
    json_files = sorted(root.rglob("combined_results*.json"))
    csv_files = sorted(root.rglob("combined_results*.csv"))
    if include_csv:
        return sorted(set(json_files + csv_files))

    # Prefer JSON; include CSV only if matching JSON not present.
    json_stems = {p.with_suffix("") for p in json_files}
    only_csv = [p for p in csv_files if p.with_suffix("") not in json_stems]
    return sorted(json_files + only_csv)


def _read_sqlite_rows(sqlite_path: Path) -> List[Tuple[Dict[str, Any], dt.datetime, str]]:
    if not sqlite_path.exists():
        return []
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    out = []
    try:
        cur = conn.cursor()
        for table in ("offers", "flights"):
            try:
                cur.execute(f"SELECT * FROM {table}")
                rows = cur.fetchall()
            except Exception:
                continue
            for r in rows:
                row = dict(r)
                ts = _to_datetime(_first(row, ["created_at", "run_ts"])) or dt.datetime.fromtimestamp(sqlite_path.stat().st_mtime)
                out.append((row, ts, f"legacy/sqlite/{sqlite_path.name}:{table}"))
    finally:
        conn.close()
    return out


def main():
    args = parse_args()
    init_db(create_tables=True)

    archive_root = Path(args.archive_root)
    files = _collect_archive_files(archive_root, include_csv=args.include_csv) if archive_root.exists() else []
    if args.limit_files and args.limit_files > 0:
        files = files[: args.limit_files]

    total_core = 0
    total_meta = 0
    total_skipped = 0
    total_sources = 0

    for p in files:
        rows = _iter_json_rows(p) if p.suffix.lower() == ".json" else _iter_csv_rows(p)
        if not rows:
            continue
        scraped_at = _parse_source_timestamp(p)
        label = f"legacy/archive/{p.as_posix()}"
        res = _process_rows(rows=rows, source_label=label, scraped_at=scraped_at, dry_run=args.dry_run)
        total_sources += 1
        total_core += res["inserted_core"] if not args.dry_run else res["core_candidates"]
        total_meta += res["inserted_meta"] if not args.dry_run else res["meta_candidates"]
        total_skipped += res["skipped"]
        print(
            f"[archive] {p} core={res['core_candidates']} meta={res['meta_candidates']} "
            f"skipped={res['skipped']} inserted_core={res['inserted_core']} inserted_meta={res['inserted_meta']}"
        )

    sqlite_rows = _read_sqlite_rows(Path(args.sqlite_path))
    if sqlite_rows:
        grouped: Dict[Tuple[dt.datetime, str], List[Dict[str, Any]]] = {}
        for row, ts, label in sqlite_rows:
            grouped.setdefault((ts, label), []).append(row)
        for (ts, label), rows in grouped.items():
            res = _process_rows(rows=rows, source_label=label, scraped_at=ts, dry_run=args.dry_run)
            total_sources += 1
            total_core += res["inserted_core"] if not args.dry_run else res["core_candidates"]
            total_meta += res["inserted_meta"] if not args.dry_run else res["meta_candidates"]
            total_skipped += res["skipped"]
            print(
                f"[sqlite] {label} core={res['core_candidates']} meta={res['meta_candidates']} "
                f"skipped={res['skipped']} inserted_core={res['inserted_core']} inserted_meta={res['inserted_meta']}"
            )

    mode = "DRY-RUN" if args.dry_run else "APPLY"
    print(
        f"Done ({mode}) sources={total_sources} total_core={total_core} total_meta={total_meta} total_skipped={total_skipped}"
    )


if __name__ == "__main__":
    main()
