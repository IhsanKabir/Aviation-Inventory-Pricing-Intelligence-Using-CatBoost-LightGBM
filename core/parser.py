# core/parser.py
def parse_biman_response(resp_json):
    rows = []
    try:
        original = resp_json["data"]["bookingAirSearch"]["originalResponse"]
    except Exception:
        return rows

    # unbundledOffers block contains offers; briefer path also exists
    offers = []
    try:
        offers = original.get("unbundledOffers", [])
        # offers is a list-of-lists in the current responses
        if offers and isinstance(offers[0], list):
            offers = offers[0]
    except Exception:
        offers = []

    for o in offers:
        try:
            brand = o.get("brandId")
            total_alt = o.get("total", {}).get("alternatives", [])
            price = None
            if total_alt and isinstance(total_alt[0], list) and total_alt[0]:
                price = total_alt[0][0].get("amount")
            # itineraryPart -> list contains segments
            it_parts = o.get("itineraryPart", [])
            seg0 = it_parts[0]["segments"][0] if it_parts and it_parts[0].get("segments") else {}
            dep = seg0.get("departure")
            arr = seg0.get("arrival")
            flight = seg0.get("flight", {})
            flight_no = flight.get("flightNumber")
            airline_code = flight.get("airlineCode")
            equipment = seg0.get("equipment")
            baggage = None
            # try retrieve baggage from fare family marketingTexts if available (fallback)
            rows.append({
                "brand": brand,
                "price": price,
                "departure": dep,
                "arrival": arr,
                "flight_number": flight_no,
                "airline_code": airline_code,
                "equipment": equipment,
                "baggage": baggage,
                "raw_offer": o
            })
        except Exception:
            continue
    return rows
