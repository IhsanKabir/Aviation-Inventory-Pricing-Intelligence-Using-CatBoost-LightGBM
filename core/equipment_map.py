EQ = {
  "738": "Boeing 737-800",
  "320": "Airbus A320",
  "321": "Airbus A321",
  "788": "Boeing 787-8",
  "DH8": "De Havilland Dash 8",
  # add more as needed
}

def pretty(code):
    return EQ.get(code, "Unknown ("+str(code)+")")
