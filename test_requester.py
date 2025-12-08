from utils.requester import send_graphql

dummy_url = "https://booking.biman-airlines.com/api/graphql"
dummy_payload = {"query": "query { __typename }"}

print(send_graphql(dummy_url, dummy_payload))
