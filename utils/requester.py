import socket
socket.setdefaulttimeout(10)

# Force Google DNS
dns_resolver = ["8.8.8.8"]



import json
import requests
import os


def load_payload(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def send_request(payload):
    url = "https://www.biman-airlines.com/graphql"

    headers = {
        "Content-Type": "application/json",
        "Origin": "https://www.biman-airlines.com",
        "Referer": "https://www.biman-airlines.com/"
    }

    try:
        return requests.post(url, json=payload, headers=headers, timeout=30)
    except Exception as e:
        print("Request failed:", e)
        return None
