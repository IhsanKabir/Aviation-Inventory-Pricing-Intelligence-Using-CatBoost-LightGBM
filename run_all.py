import os
import json
import csv
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from modules.biman import run_search as run_biman_search

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(ROOT, "config")
OUTPUT_DIR = os.path.join(ROOT, "output", "latest")
ARCHIVE_DIR = os.path.join(ROOT, "output", "archive")

ROUTES_FILE = os.path.join(CONFIG_DIR, "routes.json")
PASSENGER_FILE = os.path.join(CONFIG_DIR, "passenger.json")
SETTINGS_FILE = os.path.join(CONFIG_DIR, "settings.json")

COMBINED_JSON = os.path.join(OUTPUT_DIR, "combined_results.json")
COMBINED_CSV = os.path.join(OUTPUT_DIR, "combined_results.csv")

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s %(message)s",
)
log = logging.getLogger("run_all")

# ----------------------------
# Helpers
# ----------------------------
def load_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)


def auto_dates():
    """Generate: today, today+3, today+7, today+15"""
    base = datetime.now().date()
    return [
        str(base),
        str(base + timedelta(days=3)),
        str(base + timedelta(days=7)),
        str(base + timedelta(days=15)),
    ]


def build_tasks():
    """
    Build a list of tasks:
    [
      {
        "airline": "biman",
        "origin": "DAC",
        "destination": "CXB",
        "date": "2025-12-15",
        "cabin": "Economy",
        ...
      }
    ]
    """

    routes = load_json(ROUTES_FILE)
    passenger_cfg = load_json(PASSENGER_FILE)
    settings = load_json(SETTINGS_FILE)

    passenger_counts = passenger_cfg.get("passengers", {"ADT": 1})
    cabins = settings.get("cabins", ["Economy", "Business"])

    # Auto OR config dates
    if settings.get("auto_dates_enabled", True):
        date_list = auto_dates()
    else:
        date_list = settings.get("fixed_dates", auto_dates())

    tasks = []

    for route in routes:
        origin = route["origin"]
        destination = route["destination"]

        for dt in date_list:
            for cabin in cabins:
                tasks.append({
                    "airline": "biman",
                    "origin": origin,
                    "destination": destination,
                    "date": dt,
                    "cabin": cabin,
                    "passengers": passenger_counts,
                })

    log.info(f"Built {len(tasks)} tasks")
    return tasks


def run_task(task):
    """
    Wrapper runner so run_all does not depend on airline logic.
    """

    airline = task["airline"]

    try:
        if airline == "biman":
            ok, result = run_biman_search(
                origin=task["origin"],
                destination=task["destination"],
                date=task["date"],
                cabin=task["cabin"],
                passengers=task["passengers"],
                verbose=False,
            )
        else:
            return {
                **task,
                "ok": False,
                "error": f"Unsupported airline: {airline}"
            }

        return {
            **task,
            "ok": ok,
            "result": result,
        }

    except Exception as e:
        return {
            **task,
            "ok": False,
            "error": str(e),
        }


# ----------------------------
# Output Writers
# ----------------------------
def write_outputs(rows):
    """
    Saves:
      - combined_results.json
      - combined_results.csv
    """

    ensure_dirs()

    # JSON
    with open(COMBINED_JSON, "w", encoding="utf-8") as fh:
        json.dump(rows, fh, indent=2)

    # CSV
    fieldnames = [
        "airline", "origin", "destination", "date", "cabin",
        "ok", "price", "currency", "error"
    ]

    with open(COMBINED_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()

        for r in rows:
            if not r.get("ok"):
                writer.writerow({
                    "airline": r["airline"],
                    "origin": r["origin"],
                    "destination": r["destination"],
                    "date": r["date"],
                    "cabin": r["cabin"],
                    "ok": False,
                    "price": "",
                    "currency": "",
                    "error": r.get("error", ""),
                })
            else:
                # Extract price if exists
                root = r["result"]
                offers = root.get("offers", [])
                cheapest = offers[0] if offers else None

                price = cheapest["total"] if cheapest else ""
                curr = cheapest["currency"] if cheapest else ""

                writer.writerow({
                    "airline": r["airline"],
                    "origin": r["origin"],
                    "destination": r["destination"],
                    "date": r["date"],
                    "cabin": r["cabin"],
                    "ok": True,
                    "price": price,
                    "currency": curr,
                    "error": "",
                })

    log.info(f"Wrote outputs: {COMBINED_CSV} and {COMBINED_JSON}")


def archive_previous_output():
    """
    Moves latest output → archive with timestamp
    """

    if not os.path.exists(OUTPUT_DIR):
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(ARCHIVE_DIR, f"batch_{timestamp}")

    os.makedirs(dest, exist_ok=True)

    for filename in os.listdir(OUTPUT_DIR):
        src_path = os.path.join(OUTPUT_DIR, filename)
        dst_path = os.path.join(dest, filename)
        os.rename(src_path, dst_path)

    log.info(f"Archived previous batch → {dest}")


# ----------------------------
# Main Runner
# ----------------------------
def main():
    ensure_dirs()

    # 1) Archive last output
    archive_previous_output()

    # 2) Build tasks
    tasks = build_tasks()

    # 3) Execute tasks
    results = []

    settings = load_json(SETTINGS_FILE)
    concurrency = settings.get("concurrency", 6)

    log.info(f"Starting executor with concurrency={concurrency} tasks={len(tasks)}")

    with ThreadPoolExecutor(max_workers=concurrency) as exe:
        futures = {exe.submit(run_task, t): t for t in tasks}

        for fut in as_completed(futures):
            results.append(fut.result())

    # 4) Write structured output
    write_outputs(results)

    log.info(f"Done. Rows saved: {len(results)}")


if __name__ == "__main__":
    main()
