#!/usr/bin/env python3
"""
parse_response_full.py

Usage:
    python parse_response_full.py response.json
    python parse_response_full.py response.json --csv out.csv
    python parse_response_full.py response.json --xlsx out.xlsx

Outputs:
 - prints parsed rows (list of dicts)
 - saves CSV automatically (out: response_parsed.csv by default)
 - saves Excel if pandas is available and --xlsx provided (or by default response_parsed.xlsx)

Features:
 - Handles 'unbundledOffers' and 'brandedResults' responses
 - Maps equipment codes (e.g., '738' -> 'Boeing 737-800') using built-in table and optional equipment_map.json
 - Parses baggage text (e.g., "20KG baggage", "no checked baggage")
 - Defensive: won't crash if some fields absent
"""

import json
import re
import csv
import sys
import argparse
from pathlib import Path
from datetime import datetime
from html import unescape
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# -----------------------------
# Equipment (IATA) mapping - common codes
# You asked for full global mapping; this includes common types.
# You can extend by creating equipment_map.json next to this script.
# -----------------------------
DEFAULT_EQUIPMENT_MAP = {
    "738": "Boeing 737-800",
    "73H": "Boeing 737-800 (alternate)",
    "320": "Airbus A320",
    "321": "Airbus A321",
    "319": "Airbus A319",
    "321neo": "Airbus A321neo",
    "321N": "Airbus A321neo",
    "32N": "Airbus A320neo family",
    "738MAX": "Boeing 737 MAX 8",
    "737": "Boeing 737 (unspecified)",
    "77W": "Boeing 777-300ER",
    "772": "Boeing 777-200",
    "773": "Boeing 777-300",
    "788": "Boeing 787-8",
    "789": "Boeing 787-9",
    "77L": "Boeing 777-300ER",
    "744": "Boeing 747-400",
    "380": "Airbus A380",
    "330": "Airbus A330 (unspecified)",
    "332": "Airbus A330-200",
    "333": "Airbus A330-300",
    "350": "Airbus A350",
    "359": "Airbus A350-900",
    "321LR": "Airbus A321LR",
    "ATR72": "ATR 72",
    "AT72": "ATR 72",
    "DH8D": "De Havilland Dash 8 Q400",
    "CRJ": "Bombardier CRJ Series",
    "E190": "Embraer 190",
    "E195": "Embraer 195",
    "A220": "Airbus A220 (Bombardier C Series)",
    "A221": "Airbus A220-300",
    "A223": "Airbus A220-300",
    "321neoLR": "A321neo LR",
    "320neo": "A320neo",
}


# -----------------------------
# Helpers
# -----------------------------
def load_equipment_map(path: Path):
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                custom = json.load(f)
            # merge with defaults (custom overrides defaults)
            m = DEFAULT_EQUIPMENT_MAP.copy()
            m.update(custom)
            logging.info("Loaded equipment_map.json with %d entries (merged with defaults).", len(custom))
            return m
        except Exception as e:
            logging.warning("Could not load equipment_map.json: %s", e)
            return DEFAULT_EQUIPMENT_MAP
    else:
        return DEFAULT_EQUIPMENT_MAP


def map_equipment(code, equip_map):
    if not code:
        return ("", "")
    c = str(code).upper()
    # some APIs return numeric strings like '738' or '738 ' or 'B738' etc.
    c_clean = re.sub(r'[^A-Z0-9\-]', '', c)
    # direct map
    if c_clean in equip_map:
        return (c_clean, equip_map[c_clean])
    # try numeric trimmed
    num = re.search(r'(\d{2,4})', c_clean)
    if num:
        n = num.group(1)
        if n in equip_map:
            return (n, equip_map[n])
    # fallback
    return (c_clean, f"Unknown ({c_clean})")


def extract_baggage_from_fare_families(fare_families):
    """
    fare_families: list of fare family dicts with marketingTexts fields
    returns dict brandId -> baggage string (e.g., '20KG' or 'None')
    """
    res = {}
    if not fare_families:
        return res
    kg_rx = re.compile(r'(\d{1,3})\s*(?:kg|KG|Kg|kilogram)', re.IGNORECASE)
    no_baggage_rx = re.compile(r'no (?:checked )?baggage|does not allow', re.IGNORECASE)
    for fam in fare_families:
        brand = fam.get("brandId") or fam.get("brandLabel", [{}])[0].get("brandId")
        texts = fam.get("marketingTexts") or fam.get("brandLabel") or []
        combined_text = " ".join(
            [unescape(t.get("marketingText", "")) if isinstance(t, dict) else str(t) for t in texts])
        # parse
        kg = None
        m = kg_rx.search(combined_text)
        if m:
            kg = f"{m.group(1)} KG"
        elif no_baggage_rx.search(combined_text):
            kg = "0 KG"
        else:
            # sometimes marketingText contains "20KG baggage" or "20KG baggage"
            m2 = re.search(r'(\d+)\s*KG', combined_text, re.IGNORECASE)
            if m2:
                kg = f"{m2.group(1)} KG"
        res[brand] = kg or None
    return res


def pick_price(offer):
    """Return tuple (fare_amount, tax_amount, total_amount, currency) using available fields"""
    currency = None
    fare = None
    tax = None
    total = None
    # total alternatives often present
    try:
        total_alt = offer.get("total", {}).get("alternatives", [])
        if total_alt and isinstance(total_alt, list):
            # nested list -> take first numeric
            first = total_alt[0]
            if first and isinstance(first, list):
                cand = first[0]
                total = cand.get("amount")
                currency = cand.get("currency")
            elif isinstance(first, dict):
                total = first.get("amount")
                currency = first.get("currency")
    except Exception:
        total = None
    # fare
    try:
        fare_alt = offer.get("fare", {}).get("alternatives", [])
        if fare_alt and isinstance(fare_alt, list):
            first = fare_alt[0]
            if first and isinstance(first, list):
                cand = first[0]
                fare = cand.get("amount")
                if not currency:
                    currency = cand.get("currency")
            elif isinstance(first, dict):
                fare = first.get("amount")
                if not currency:
                    currency = first.get("currency")
    except Exception:
        fare = None
    # taxes
    try:
        tax_alt = offer.get("taxes", {}).get("alternatives", [])
        if tax_alt and isinstance(tax_alt, list):
            first = tax_alt[0]
            if first and isinstance(first, list):
                cand = first[0]
                tax = cand.get("amount")
                if not currency:
                    currency = cand.get("currency")
            elif isinstance(first, dict):
                tax = first.get("amount")
                if not currency:
                    currency = first.get("currency")
    except Exception:
        tax = None
    # if any missing, try to compute
    if not total and fare:
        try:
            total = (fare or 0) + (tax or 0)
        except Exception:
            pass
    return fare, tax, total, currency


def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


# -----------------------------
# Core parsing
# -----------------------------
def parse_unbundled_offers(unbundled_offers, fare_family_baggage, equip_map, rows):
    # unbundledOffers is usually a list of lists [[offer1, offer2...], [altDateOffers]]
    if not unbundled_offers:
        return
    for group in unbundled_offers:
        if not isinstance(group, list):
            continue
        for offer in group:
            try:
                brand_id = offer.get("brandId")
                seats = safe_get(offer, "seatsRemaining", "count")
                cabin = offer.get("cabinClass")
                fare_basis = None
                # itineraryPart is a list with parts; iterate segments
                itinerary_parts = offer.get("itineraryPart", []) or []
                for ip in itinerary_parts:
                    segments = ip.get("segments", []) or []
                    for seg in segments:
                        flight = seg.get("flight", {})
                        airline_code = flight.get("airlineCode")
                        operating_airline = flight.get("operatingAirlineCode")
                        flight_number = flight.get("flightNumber") or seg.get("flightNumber")
                        equipment = seg.get("equipment") or seg.get("aircraft") or seg.get("equipmentCode")
                        equip_code, equip_desc = map_equipment(equipment, equip_map)
                        origin = seg.get("origin")
                        destination = seg.get("destination")
                        dep = seg.get("departure")
                        arr = seg.get("arrival")
                        duration = seg.get("duration")
                        stops = ip.get("stops", seg.get("stops", 0))
                        booking_class = seg.get("bookingClass") or ip.get("bookingClass") or offer.get("bookingClass")
                        fare_basis = seg.get("fareBasis") or fare_basis
                        meals = seg.get("meals")
                        layover = seg.get("layoverDuration", seg.get("layoverDuration", 0))
                        # price
                        fare_amt, tax_amt, total_amt, currency = pick_price(offer)
                        baggage = None
                        # try mapping from fare_family_baggage by brand
                        if fare_family_baggage and brand_id in fare_family_baggage:
                            baggage = fare_family_baggage[brand_id]
                        rows.append({
                            "brand": brand_id,
                            "airline": airline_code,
                            "operating_airline": operating_airline,
                            "flight_number": flight_number,
                            "equipment_code": equip_code,
                            "aircraft": equip_desc,
                            "origin": origin,
                            "destination": destination,
                            "departure": dep,
                            "arrival": arr,
                            "duration_min": duration,
                            "stops": stops,
                            "cabin": cabin,
                            "booking_class": booking_class,
                            "fare_basis": fare_basis,
                            "fare_amount": fare_amt,
                            "tax_amount": tax_amt,
                            "total_amount": total_amt,
                            "currency": currency,
                            "baggage": baggage,
                            "seats_remaining": seats,
                            "raw_offer": offer
                        })
            except Exception as e:
                logging.warning("Error parsing unbundled offer: %s", e)


def parse_branded_results(branded_results, fare_family_baggage, equip_map, rows):
    """
    brandedResults -> itineraryPartBrands
    structure example:
    brandedResults: {
      "itineraryPartBrands": [
        [
          {
            "itineraryPart": {"@ref": "1"},
            "brandOffers": [ {shoppingBasketHashCode:..., brandId:..., total:..., fare:...}, ... ],
            "duration": 255, "departure": "...", "arrival": "..."
          }
        ]
      ]
    }
    """
    if not branded_results:
        return
    ipb_list = branded_results.get("itineraryPartBrands") or []
    for outer in ipb_list:
        # outer often is a list of dicts
        if isinstance(outer, list):
            for ipb in outer:
                # ipb should be dict, but sometimes it's a list
                if not isinstance(ipb, dict):
                    continue
                itinerary_part = ipb.get("itineraryPart") or {}
                dep = ipb.get("departure") or itinerary_part.get("departure")
                arr = ipb.get("arrival") or itinerary_part.get("arrival")
                duration = ipb.get("duration") or itinerary_part.get("duration")
                brand_offers = ipb.get("brandOffers") or []
                for bo in brand_offers:
                    try:
                        brand_id = bo.get("brandId")
                        seats = safe_get(bo, "seatsRemaining", "count")
                        cabin = bo.get("cabinClass")
                        fare_basis = None
                        # itineraryPart inside brandOffer
                        iparts = bo.get("itineraryPart", []) or []
                        if not iparts:
                            iparts = itinerary_part.get("segments", []) or []
                        for ip in iparts:
                            segments = ip.get("segments", []) or []
                            for seg in segments:
                                flight = seg.get("flight", {})
                                airline_code = flight.get("airlineCode")
                                operating_airline = flight.get("operatingAirlineCode")
                                flight_number = flight.get("flightNumber") or seg.get("flightNumber")
                                equipment = seg.get("equipment") or seg.get("equipmentCode")
                                equip_code, equip_desc = map_equipment(equipment, equip_map)
                                origin = seg.get("origin")
                                destination = seg.get("destination")
                                dep_seg = seg.get("departure") or dep
                                arr_seg = seg.get("arrival") or arr
                                duration_seg = seg.get("duration") or duration
                                stops = ip.get("stops", seg.get("stops", 0))
                                booking_class = seg.get("bookingClass") or bo.get("bookingClass")
                                fare_basis = seg.get("fareBasis") or fare_basis
                                fare_amt, tax_amt, total_amt, currency = pick_price(bo)
                                baggage = None
                                if fare_family_baggage and brand_id in fare_family_baggage:
                                    baggage = fare_family_baggage[brand_id]
                                rows.append({
                                    "brand": brand_id,
                                    "airline": airline_code,
                                    "operating_airline": operating_airline,
                                    "flight_number": flight_number,
                                    "equipment_code": equip_code,
                                    "aircraft": equip_desc,
                                    "origin": origin,
                                    "destination": destination,
                                    "departure": dep_seg,
                                    "arrival": arr_seg,
                                    "duration_min": duration_seg,
                                    "stops": stops,
                                    "cabin": cabin,
                                    "booking_class": booking_class,
                                    "fare_basis": fare_basis,
                                    "fare_amount": fare_amt,
                                    "tax_amount": tax_amt,
                                    "total_amount": total_amt,
                                    "currency": currency,
                                    "baggage": baggage,
                                    "seats_remaining": seats,
                                    "raw_offer": bo
                                })
                    except Exception as e:
                        logging.warning("Error parsing branded offer: %s", e)
        else:
            logging.debug("Unexpected itineraryPartBrands element type: %s", type(outer))


def parse_response(data, equip_map):
    """
    data: loaded JSON from response.json
    returns list of dict rows
    """
    rows = []
    # navigate to originalResponse where the actual content sits
    original = safe_get(data, "data", "bookingAirSearch", "originalResponse")
    if not original:
        # sometimes top-level is the original itself
        original = data.get("originalResponse") or data.get("DigitalConnectOriginalResponse") or data

    # build fare family baggage map
    fare_families = original.get("fareFamilies") or []
    fare_family_baggage = extract_baggage_from_fare_families(fare_families)

    # unbundledOffers
    unbundled = original.get("unbundledOffers") or original.get("unbundledOffers", [])
    parse_unbundled_offers(unbundled, fare_family_baggage, equip_map, rows)

    # brandedResults
    branded = original.get("brandedResults") or original.get("brandedResults", {})
    parse_branded_results(branded, fare_family_baggage, equip_map, rows)

    # some APIs put offers under 'bundledOffers' or 'bundledAlternateDateOffers'
    bundled = original.get("bundledOffers") or []
    if bundled:
        # transform to unbundled-like structure and parse
        parse_unbundled_offers([bundled], fare_family_baggage, equip_map, rows)

    # If still empty, try legacy keys
    if not rows:
        # try to dig into 'offers' or 'shoppingOffers'
        offers = safe_get(original, "offers") or safe_get(original, "shoppingOffers") or []
        if offers:
            parse_unbundled_offers([offers], fare_family_baggage, equip_map, rows)

    return rows


# -----------------------------
# Output helpers
# -----------------------------
def save_csv(rows, csv_path):
    if not rows:
        logging.info("No rows to save to CSV.")
        return
    keys = list(rows[0].keys())
    # remove raw_offer large blob for CSV (optional)
    if "raw_offer" in keys:
        keys.remove("raw_offer")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            row_copy = {k: v for k, v in r.items() if k in keys}
            # stringify nested items safely
            for k, v in row_copy.items():
                if isinstance(v, (list, dict)):
                    row_copy[k] = json.dumps(v, ensure_ascii=False)
            w.writerow(row_copy)
    logging.info("Saved CSV: %s (%d rows)", csv_path, len(rows))


def save_excel(rows, xlsx_path):
    try:
        import pandas as pd
    except Exception as e:
        logging.warning("Pandas not installed. Skipping Excel output. Install pandas to enable (.xlsx).")
        return False
    if not rows:
        logging.info("No rows to save to Excel.")
        return True
    # convert rows to DataFrame, remove raw_offer
    df_rows = []
    for r in rows:
        r2 = r.copy()
        if "raw_offer" in r2:
            del r2["raw_offer"]
        df_rows.append(r2)
    df = pd.DataFrame(df_rows)
    df.to_excel(xlsx_path, index=False)
    logging.info("Saved Excel: %s (%d rows)", xlsx_path, len(df))
    return True


# -----------------------------
# CLI / Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Parse booking API response and extract flight offers.")
    parser.add_argument("response", help="response.json (GraphQL JSON capture)")
    parser.add_argument("--csv", help="CSV output path (default: response_parsed.csv)", default=None)
    parser.add_argument("--xlsx", help="Excel output path (default: response_parsed.xlsx)", default=None)
    parser.add_argument("--equipment-map", help="Optional custom equipment_map.json path", default="equipment_map.json")
    args = parser.parse_args()

    resp_path = Path(args.response)
    if not resp_path.exists():
        logging.error("Response file not found: %s", resp_path)
        sys.exit(1)

    out_csv = args.csv or (resp_path.with_name(resp_path.stem + "_parsed.csv"))
    out_xlsx = args.xlsx or (resp_path.with_name(resp_path.stem + "_parsed.xlsx"))

    # load equipment map (merge custom if present)
    equip_map = load_equipment_map(Path(args.equipment_map))

    # load response JSON
    with open(resp_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = parse_response(data, equip_map)

    if not rows:
        logging.warning("No usable flight rows extracted.")
    else:
        # print a compact summary (first 10 rows)
        print("\nParsed rows (first 20 shown):")
        for i, r in enumerate(rows[:20], 1):
            # choose compact display
            print(
                f"{i:02d}) {r.get('airline')} {r.get('flight_number')} | {r.get('origin')}->{r.get('destination')} | {r.get('departure')} → {r.get('arrival')} | Eq: {r.get('equipment_code')} ({r.get('aircraft')}) | Brand: {r.get('brand')} | Price: {r.get('total_amount')} {r.get('currency')} | Baggage: {r.get('baggage')}")
        print(f"... total rows: {len(rows)}\n")

    # Save CSV
    save_csv(rows, out_csv)

    # Save Excel if pandas available
    save_excel(rows, out_xlsx)

    # Also save a full JSON of parsed rows for downstream consumption
    json_out = resp_path.with_name(resp_path.stem + "_parsed.json")
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    logging.info("Saved parsed JSON: %s", json_out)


if __name__ == "__main__":
    main()
