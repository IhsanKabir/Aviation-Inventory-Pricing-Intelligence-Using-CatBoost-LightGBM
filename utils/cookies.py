import json
import os
import time


def load_cookies(path="state.json"):
    """
    Loads cookies exported from your browser (state.json).
    Converts list of cookies → dict usable by requests.
    Also removes expired cookies.
    """

    if not os.path.exists(path):
        raise FileNotFoundError("❌ state.json not found. Generate cookies first.")

    with open(path, "r") as f:
        data = json.load(f)

    if "cookies" not in data:
        raise ValueError("❌ state.json missing 'cookies' field.")

    now = int(time.time())
    cookies = {}

    for c in data["cookies"]:
        # filter expired cookies
        exp = c.get("expires", None)
        if exp and isinstance(exp, (int, float)) and exp < now:
            continue

        name = c.get("name")
        value = c.get("value")

        if name and value:
            cookies[name] = value

    return cookies
