# modules/biman.py
"""
Biman client module - Option C (keep alternate-date rows) + F1 (one row per brand).
Features:
 - HAR-accurate headers (ADRUM, application-id, conversation-id, execution)
 - verbose / curl mode
 - saves raw responses to debug/raw/
 - parses unbundledOffers and unbundledAlternateDateOffers -> one row per brand/offer
 - auto-shifts date on validation error (tries next day up to max_shift_days)
 - emits "no-flight" row with reason when no offers found
 - compatible with core.requester.Requester interface used in your codebase
"""

from __future__ import annotations
import json
import logging
import os
import uuid
import datetime
from typing import Any, Dict, List, Optional

from core.requester import Requester, RequesterError  # expect this to exist in your repo

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

GRAPHQL_ENDPOINT = "https://booking.biman-airlines.com/api/graphql"
PREFLIGHT_URL = "https://booking.biman-airlines.com/dx/BGDX/"

DEFAULT_COOKIES_PATH = os.path.join(os.path.dirname(__file__), "..", "cookies", "biman.json")
DEBUG_RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "debug", "raw")

# Static HAR headers
STATIC_HAR_HEADERS = {
    "Accept": "application/json",
    "Origin": "https://booking.biman-airlines.com",
    "Referer": "https://booking.biman-airlines.com/dx/BGDX/",
    "Content-Type": "application/json",
    "x-sabre-storefront": "BGDX",
    "ADRUM": "isAjax:true",
    "application-id": "SWS1:SBR-GCPDCShpBk:2ceb6478a8",
}

# ensure debug dir
os.makedirs(DEBUG_RAW_DIR, exist_ok=True)

class BimanClient:
    def __init__(
        self,
        requester: Optional[Requester] = None,
        verbose: bool = False,
        curl_mode: bool = False,
        cookies_path: Optional[str] = None,
        raw_save: bool = True,
    ):
        self.verbose = verbose
        self.curl_mode = curl_mode
        self.raw_save = raw_save
        self.cookies_path = cookies_path or DEFAULT_COOKIES_PATH
        self.r = requester or Requester()

        loaded = False
        try:
            loaded = self.r.load_static_cookies(self.cookies_path)
        except Exception:
            loaded = False

        if loaded:
            logger.info("Loaded cookies for module biman from %s", self.cookies_path)
        else:
            logger.info("No cookies – running preflight GET to generate cookies")
            try:
                self.r.generate_new_cookies(PREFLIGHT_URL, headers={"Referer": STATIC_HAR_HEADERS["Referer"]})
            except Exception:
                logger.exception("Preflight cookie generation failed")

    # build headers with dynamic conversation-id/execution
    def _make_headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        h = dict(STATIC_HAR_HEADERS)
        h.setdefault("accept", "*/*")
        h["conversation-id"] = str(uuid.uuid4())
        h["execution"] = str(uuid.uuid4())
        if extra:
            h.update(extra)
        return h

    def _build_payload(self, air_search_input: Dict[str, Any]) -> Dict[str, Any]:
        query = (
            "query bookingAirSearch($airSearchInput: CustomAirSearchInput) {\n"
            "  bookingAirSearch(airSearchInput: $airSearchInput) {\n"
            "    originalResponse\n"
            "    __typename\n"
            "  }\n"
            "}"
        )
        return {
            "operationName": "bookingAirSearch",
            "query": query,
            "variables": {"airSearchInput": air_search_input},
            "extensions": {},
        }

    def _print_curl(self, url: str, headers: Dict[str, str], payload: Dict[str, Any]):
        hdrs = [f"-H {json.dumps(f'{k}: {v}')}" for k, v in headers.items()]
        data = json.dumps(payload, ensure_ascii=False)
        curl_cmd = (
            f"curl -X POST {url} \\\n  "
            + " \\\n  ".join(hdrs)
            + f" \\\n  --data-raw {json.dumps(data)} --compressed"
        )
        logger.debug("cURL (approx):\n%s", curl_cmd)
        if self.curl_mode:
            print("\n# ----- cURL (Biman GraphQL) -----")
            print(curl_cmd)
            print("# ----- end cURL -----\n")

    def _save_raw(self, origin: str, destination: str, requested_date: str, obj: Any):
        if not self.raw_save:
            return
        fname = f"{origin}-{destination}-{requested_date}-{uuid.uuid4().hex}.json"
        path = os.path.join(DEBUG_RAW_DIR, fname)
        try:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(obj, fh, ensure_ascii=False, indent=2)
            logger.debug("Saved raw response to %s", path)
        except Exception:
            logger.exception("Failed to save raw response to %s", path)

    # Helper to extract numeric price (first alt)
    def _get_price_and_currency(self, obj: Dict[str, Any]) -> (Optional[int], Optional[str]):
        try:
            alts = obj.get("total", {}).get("alternatives", [])
            if alts and isinstance(alts, list) and alts[0] and isinstance(alts[0], list):
                item = alts[0][0]
                return item.get("amount"), item.get("currency")
            # fallback to fare.alternatives
            alts = obj.get("fare", {}).get("alternatives", [])
            if alts and alts[0] and isinstance(alts[0], list):
                item = alts[0][0]
                return item.get("amount"), item.get("currency")
        except Exception:
            pass
        return None, None

    # parse a single offer (brand-level) -> row dict
    def _offer_to_row(
        self,
        origin: str,
        destination: str,
        requested_date: str,
        actual_date: str,
        source: str,
        brand_offer: Dict[str, Any],
        itinerary_index: int = 0,
    ) -> Dict[str, Any]:
        amount, currency = self._get_price_and_currency(brand_offer)
        seats = None
        seats_info = brand_offer.get("seatsRemaining")
        if isinstance(seats_info, dict):
            seats = seats_info.get("count")

        # try to find itineraryPart -> segments -> first segment
        seg = {}
        try:
            it_parts = brand_offer.get("itineraryPart") or brand_offer.get("itineraryPart")
            if isinstance(it_parts, list) and len(it_parts) > 0:
                segs = it_parts[0].get("segments") or []
                if segs:
                    seg = segs[0]
        except Exception:
            seg = {}

        flight = seg.get("flight", {}) or {}
        departure = seg.get("departure")
        arrival = seg.get("arrival")
        booking_class = seg.get("bookingClass") or brand_offer.get("bookingClass")

        row = {
            "requested_date": requested_date,
            "actual_date": actual_date,
            "source": source,  # 'exact' or 'alternate'
            "origin": origin,
            "destination": destination,
            "brandId": brand_offer.get("brandId"),
            "shoppingBasketHashCode": brand_offer.get("shoppingBasketHashCode"),
            "soldout": brand_offer.get("soldout"),
            "cabinClass": brand_offer.get("cabinClass"),
            "seatsRemaining": seats,
            "price": amount,
            "currency": currency,
            "departure": departure,
            "arrival": arrival,
            "flightNumber": flight.get("flightNumber"),
            "operatingFlightNumber": flight.get("operatingFlightNumber"),
            "airlineCode": flight.get("airlineCode"),
            "bookingClass": booking_class,
            "fareBasis": seg.get("fareBasis"),
            "raw_offer": brand_offer,
        }
        return row

    # parse entire originalResponse (dict) -> rows list
    def _parse_original_response(self, origin: str, destination: str, requested_date: str, original_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        # main body can contain 'unbundledOffers' (list-of-lists) and 'unbundledAlternateDateOffers'
        #  - unbundledOffers: offers for requested date (list-of-lists: itinerary combos)
        #  - unbundledAlternateDateOffers: list-of-lists or lists with departureDates (alternate date offers)

        # 1) exact offers
        u_offers = original_obj.get("unbundledOffers") or []
        # u_offers could be list of groups -> each group has brand objects
        for group in u_offers:
            if not isinstance(group, list):
                continue
            for offer in group:
                # each offer is a brand-level object
                try:
                    row = self._offer_to_row(origin, destination, requested_date, requested_date, "exact", offer)
                    rows.append(row)
                except Exception:
                    logger.exception("Failed to parse exact offer")

        # 2) unbundledAlternateDateOffers -> offers for other dates (C-mode: keep ALL)
        alt = original_obj.get("unbundledAlternateDateOffers") or []
        for group in alt:
            if not isinstance(group, list):
                continue
            for offer in group:
                # each offer may include 'departureDates' - iterate departureDates if present
                departure_dates = offer.get("departureDates") or []
                if departure_dates:
                    for d in departure_dates:
                        try:
                            row = self._offer_to_row(origin, destination, requested_date, d, "alternate", offer)
                            rows.append(row)
                        except Exception:
                            logger.exception("Failed to parse alternate offer with departureDates")
                else:
                    # no explicit departureDates; try to extract departure from itinerary
                    try:
                        # try to get first segment departure datetime and take date part
                        it_parts = offer.get("itineraryPart") or []
                        dt = None
                        if isinstance(it_parts, list) and it_parts:
                            segs = it_parts[0].get("segments") or []
                            if segs:
                                dt = segs[0].get("departure")
                        actual_date = dt.split("T")[0] if isinstance(dt, str) and "T" in dt else requested_date
                        row = self._offer_to_row(origin, destination, requested_date, actual_date, "alternate", offer)
                        rows.append(row)
                    except Exception:
                        logger.exception("Failed to parse alternate offer without departureDates")

        return rows

    # Determine if response is a validation error for date
    def _response_has_date_validation_error(self, resp: Dict[str, Any]) -> bool:
        # the earlier responses showed structure like:
        # 'extensions': { 'errors': [{'responseData': {'type':'Validation', 'details': {'itineraryParts[0].when.date': ['invalid.date|YYYY-MM-DD']}}}]}
        try:
            ex = resp.get("extensions") or {}
            errs = ex.get("errors") or []
            for e in errs:
                rd = e.get("responseData") or {}
                if rd.get("type") == "Validation":
                    details = rd.get("details") or {}
                    for k, v in details.items():
                        if "when.date" in k or "itineraryParts" in k:
                            return True
        except Exception:
            pass
        return False

    # main search function (tries auto-shift on validation errors)
    def search(self, air_search_input: Dict[str, Any], max_shift_days: int = 7) -> Dict[str, Any]:
        """
        Input: air_search_input = GraphQL input (as your earlier module passed)
        Returns: {"ok": True, "rows": [..], "raw": <originalResponse> } OR {"ok": False, "error": "..."}
        Behavior:
         - if API returns validation error for requested date, will attempt next day (up to max_shift_days)
         - saves raw response to debug/raw/
        """
        try:
            itin = air_search_input.get("itineraryParts", [{}])[0]
            origin = itin.get("from", {}).get("code")
            destination = itin.get("to", {}).get("code")
            requested_date = itin.get("when", {}).get("date")
            cabin = air_search_input.get("cabinClass")
        except Exception:
            origin = air_search_input.get("origin")
            destination = air_search_input.get("destination")
            requested_date = air_search_input.get("date")
            cabin = air_search_input.get("cabinClass")

        logger.info("Biman search %s -> %s on %s cabin=%s", origin, destination, requested_date, cabin)

        payload = self._build_payload(air_search_input)
        headers = self._make_headers()

        if self.verbose:
            logger.debug("Request headers:\n%s", json.dumps(headers, indent=2))
            logger.debug("Payload:\n%s", json.dumps(payload, indent=2))

        if self.curl_mode:
            self._print_curl(GRAPHQL_ENDPOINT, headers, payload)

        tries = 0
        last_response = None
        current_date = datetime.date.fromisoformat(requested_date)
        shift_attempts = 0

        while True:
            tries += 1
            # update payload variables to current_date
            payload["variables"]["airSearchInput"]["itineraryParts"][0]["when"]["date"] = current_date.isoformat()
            if self.curl_mode:
                self._print_curl(GRAPHQL_ENDPOINT, headers, payload)
            try:
                response_json = self.r.send_graphql(
                    GRAPHQL_ENDPOINT,
                    payload["query"],
                    variables=payload["variables"],
                    headers=headers,
                )
                last_response = response_json
                # Normalize originalResponse if string
                booking = None
                if isinstance(response_json, dict):
                    data = response_json.get("data") or {}
                    booking = data.get("bookingAirSearch")
                if not booking:
                    # If server returned data with bookingAirSearch: None but with extensions showing validation error, handle below
                    if isinstance(response_json, dict) and self._response_has_date_validation_error(response_json.get("extensions") or response_json):
                        # validation error for date -> shift forward
                        shift_attempts += 1
                        if shift_attempts > max_shift_days:
                            logger.warning("Max shift attempts reached (%s). Returning validation error.", max_shift_days)
                            break
                        logger.info("Validation error for date %s - auto-shifting to %s", current_date.isoformat(), (current_date + datetime.timedelta(days=1)).isoformat())
                        current_date = current_date + datetime.timedelta(days=1)
                        continue
                    # otherwise record no bookingAirSearch
                    logger.warning("No bookingAirSearch in response: %s", response_json)
                    # Save raw anyway
                    self._save_raw(origin, destination, current_date.isoformat(), response_json)
                    return {"ok": False, "error": "No bookingAirSearch", "raw": response_json}

                # booking exists: booking is likely a dict containing 'originalResponse'
                original = booking.get("originalResponse")
                # sometimes original is stringified JSON
                if isinstance(original, str):
                    try:
                        original_obj = json.loads(original)
                    except Exception:
                        original_obj = original
                else:
                    original_obj = original

                # Save raw response
                self._save_raw(origin, destination, current_date.isoformat(), booking)

                # Parse into rows
                rows = []
                try:
                    rows = self._parse_original_response(origin, destination, requested_date, original_obj or {})
                except Exception:
                    logger.exception("Failed parsing original response")

                # If no rows (no offers), attempt auto-shift to next day (as user requested) up to max_shift_days
                if not rows:
                    shift_attempts += 1
                    if shift_attempts <= max_shift_days:
                        logger.info("No flights found for %s - shifting to next day %s (attempt %s/%s)", current_date.isoformat(), (current_date + datetime.timedelta(days=1)).isoformat(), shift_attempts, max_shift_days)
                        current_date = current_date + datetime.timedelta(days=1)
                        continue
                    else:
                        # record no-flight monitoring row
                        nf_row = {
                            "requested_date": requested_date,
                            "actual_date": current_date.isoformat(),
                            "source": "none",
                            "origin": origin,
                            "destination": destination,
                            "brandId": None,
                            "reason": "NO_FLIGHTS_FOUND",
                            "raw_offer": original_obj,
                        }
                        return {"ok": True, "rows": [nf_row], "raw": booking}
                else:
                    # success - save cookies then return rows
                    try:
                        self.r.save_cookies(self.cookies_path)
                    except Exception:
                        logger.debug("Failed to save cookies (non-fatal)")
                    return {"ok": True, "rows": rows, "raw": booking}

            except RequesterError as e:
                msg = str(e)
                logger.error("RequesterError: %s", msg)
                # If 400 Bad Request -> preflight and retry (like earlier)
                if "400" in msg or "Bad Request" in msg:
                    logger.info("400 Bad Request received - refreshing cookies and retrying")
                    try:
                        self.r.generate_new_cookies(PREFLIGHT_URL, headers={"Referer": STATIC_HAR_HEADERS["Referer"]})
                    except Exception:
                        logger.exception("Preflight GET failed")
                    # after refresh, attempt next loop (same current_date)
                    shift_attempts += 1
                    if shift_attempts > max_shift_days:
                        return {"ok": False, "error": f"RequesterError after refresh: {msg}"}
                    continue
                return {"ok": False, "error": msg}
            except Exception as e:
                logger.exception("Unexpected error during search")
                return {"ok": False, "error": f"Unexpected: {e}"}

    # run_search wrapper for backward compatibility with run_all
    def run_search(self, task: Dict[str, Any]) -> Dict[str, Any]:
        # normalize incoming task -> air_search_input
        if "itineraryParts" in task:
            return self.search(task)
        origin = task.get("origin") or task.get("from")
        destination = task.get("destination") or task.get("to")
        date = task.get("date")
        cabin = task.get("cabin") or "Economy"
        passengers = task.get("passengers") or {"ADT": 1}
        air_input = {
            "cabinClass": cabin,
            "awardBooking": False,
            "promoCodes": [],
            "searchType": "BRANDED",
            "itineraryParts": [
                {
                    "from": {"useNearbyLocations": False, "code": origin},
                    "to": {"useNearbyLocations": False, "code": destination},
                    "when": {"date": date},
                }
            ],
            "passengers": passengers,
        }
        return self.search(air_input)


# singleton helper
_CLIENT: Optional[BimanClient] = None

def get_client(**kwargs) -> BimanClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = BimanClient(**kwargs)
    return _CLIENT

def run_search(task: Dict[str, Any]) -> Dict[str, Any]:
    return get_client().run_search(task)


# CLI for debug
if __name__ == "__main__":
    import argparse
    import sys
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--origin", required=True)
    p.add_argument("--destination", required=True)
    p.add_argument("--date", required=True)
    p.add_argument("--cabin", default="Economy")
    p.add_argument("--adt", type=int, default=1)
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--curl", action="store_true")
    p.add_argument("--cookies", default=None)
    args = p.parse_args(sys.argv[1:])

    client = get_client(requester=Requester(), verbose=args.verbose, curl_mode=args.curl, cookies_path=args.cookies)
    task = {
        "origin": args.origin,
        "destination": args.destination,
        "date": args.date,
        "cabin": args.cabin,
        "passengers": {"ADT": args.adt},
        "airline_code": "BG",
    }
    res = client.run_search(task)
    print(json.dumps(res, indent=2, ensure_ascii=False))
