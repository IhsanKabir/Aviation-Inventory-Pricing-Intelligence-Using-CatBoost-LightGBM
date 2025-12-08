import os
import logging
from core.requester import Requester

LOG = logging.getLogger(__name__)


AIRLINE_CODE = "BG"
AIRLINE_NAME = "Biman Bangladesh Airlines"


# ----------------------------------------
# Build Payload (FIXED)
# ----------------------------------------
def build_payload(base_payload, origin, destination, date):
    """
    Returns a SAFE, CLEAN Sabre GraphQL payload
    All structure issues fixed:
        - itineraryParts must be a LIST
        - useNearbyLocations: False for both
        - copying base payload structure
    """
    p = json.loads(json.dumps(base_payload))  # deep copy

    p["variables"]["airSearchInput"]["itineraryParts"] = [
        {
            "from": {"useNearbyLocations": False, "code": origin},
            "to": {"useNearbyLocations": False, "code": destination},
            "when": {"date": date}
        }
    ]

    return p


# ----------------------------------------
# Parse GraphQL response safely
# ----------------------------------------
def parse_flights(js, origin, destination, date):
    """
    Parses the returned JSON safely.
    If no data, returns empty list.
    """

    if not isinstance(js, dict):
        return []

    data = js.get("data")
    if not data:
        return []

    result = data.get("bookingAirSearch")
    if not result:
        return []

    priced = result.get("pricedItineraries", [])
    rows = []

    for entry in priced:
        price_info = entry.get("pricing", {})
        total_price = price_info.get("totalPrice")

        # Each itinerary should have segments
        itinerary = entry.get("itinerary", {})
        segs = itinerary.get("segments", [])

        flights = []
        for s in segs:
            flights.append({
                "flightNumber": s.get("flightNumber"),
                "carrier": s.get("carrier"),
                "from": s.get("from", {}).get("code"),
                "to": s.get("to", {}).get("code"),
                "departure": s.get("departureDateTime"),
                "arrival": s.get("arrivalDateTime")
            })

        rows.append({
            "origin": origin,
            "destination": destination,
            "date": date,
            "price": total_price,
            "flights": flights
        })

    return rows


# ----------------------------------------
# Main data fetcher
# ----------------------------------------
def fetch_flights(origin, destination, date):
    """
    Called by run_all.py for each route + date.
    Fully hardened.
    """

    # Load cookies (optional)
    try:
        cookies = load_cookies_state()
    except Exception:
        cookies = {}
        print(f"[{AIRLINE_CODE}] ⚠ No valid state.json cookie found – continuing without cookies")

    # Load BASE payload from payload.json
    base = load_payload()

    # Build correct Sabre payload
    payload = build_payload(base, origin, destination, date)

    # Perform request
    try:
        js = send_biman_request(payload, cookies)
    except Exception as e:
        print(f"[{AIRLINE_CODE}] ❌ Request failed: {e}")
        return []

    return parse_flights(js, origin, destination, date)
