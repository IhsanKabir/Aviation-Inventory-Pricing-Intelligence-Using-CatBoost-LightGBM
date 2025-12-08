from datetime import datetime, timedelta

def build_dates_from_offsets(offsets):
    today = datetime.today().date()
    result = []
    for off in offsets:
        d = today + timedelta(days=off)
        result.append(d.strftime("%Y-%m-%d"))
    return result
