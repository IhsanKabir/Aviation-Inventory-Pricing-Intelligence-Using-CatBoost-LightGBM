# tools/payload_validator.py
import json
import sys
from pathlib import Path

def validate_payload(path="payload.json"):
    p = Path(path)
    if not p.exists():
        print("❌ payload.json not found at", p.resolve())
        return 2
    data = json.loads(p.read_text(encoding="utf-8"))

    vars = data.get("variables")
    if not vars:
        print("❌ Missing top-level 'variables' key.")
        return 3

    # Two allowed shapes:
    a = vars.get("input")
    b = vars.get("searchQuery")

    ok = False
    if isinstance(a, dict) and isinstance(a.get("originDestinations"), list):
        ok = True
        print("✅ Found variables.input.originDestinations (OK).")
    if isinstance(b, dict) and isinstance(b.get("originDestinations"), list):
        ok = True
        print("✅ Found variables.searchQuery.originDestinations (OK).")

    if not ok:
        # print suggestions
        print("❌ No originDestinations found under variables.input or variables.searchQuery.")
        print("Expected JSON snippet example:")
        print('''{
  "variables": {
    "input": {
      "originDestinations": [
        {
          "originLocationCode": "DAC",
          "destinationLocationCode": "CGP",
          "departureDate": "2025-12-04"
        }
      ]
    }
  }
}''')
        return 4

    print("Payload validation PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(validate_payload())
