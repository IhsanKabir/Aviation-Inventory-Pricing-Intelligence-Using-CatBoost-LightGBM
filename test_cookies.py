from utils.cookies import load_cookies

try:
    cookies = load_cookies()
    print("\n✅ Cookie module works correctly!")
    print(f"Loaded {len(cookies)} cookies:")
    for k, v in cookies.items():
        print(f"  {k} = {v[:20]}...")   # show first 20 chars
except Exception as e:
    print("\n❌ Cookie module has an error:")
    print(e)
