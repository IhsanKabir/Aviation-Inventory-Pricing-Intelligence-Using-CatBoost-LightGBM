from utils.requester import send_graphql
import json

# Load the REAL working payload
with open("payload.json", "r") as f:
    payload = json.load(f)

url = "https://booking.biman-airlines.com/api/graphql"

result = send_graphql(url, payload)
print(result)
