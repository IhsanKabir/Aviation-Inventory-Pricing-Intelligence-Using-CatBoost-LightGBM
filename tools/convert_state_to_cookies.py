# tools/convert_state_to_cookies.py
import json
from pathlib import Path

# adapt these paths to your environment
candidates = [
    Path("core/state.json"),
    Path("state.json"),
    Path("airline_scraper/state.json"),
]

out = Path("core/cookies.json")
for p in candidates:
    if p.exists():
        d = json.loads(p.read_text(encoding="utf-8"))
        # heuristic: if 'cookies' key exists, use it; otherwise use full dict
        cookies = d.get("cookies") if isinstance(d, dict) and d.get("cookies") else d
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(cookies, indent=2), encoding="utf-8")
        print("Wrote", out, "from", p)
        break
else:
    print("No state.json found - please provide file")
