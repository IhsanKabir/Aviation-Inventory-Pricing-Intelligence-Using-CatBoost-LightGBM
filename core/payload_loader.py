import json
import os

def load_payload(name="biman"):
    """
    Loads payload.json from the project root or config folder.
    Returns a Python dict.
    """
    # Common search paths
    candidates = [
        os.path.join(os.getcwd(), "payload.json"),
        os.path.join(os.getcwd(), "config", "payload.json"),
        os.path.join(os.path.dirname(__file__), "..", "payload.json"),
        os.path.join(os.path.dirname(__file__), "payload.json"),
    ]

    for path in candidates:
        path = os.path.abspath(path)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"[payload_loader] Failed to load {path}:", e)

    print("[payload_loader] ERROR: payload.json not found in expected locations.")
    return {}
