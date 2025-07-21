import requests
import pandas as pd
import json
import os

# === CONFIG ===
HOST = "https://api.withflex.com"  # Replace with actual host
EMAIL = "miguel@withflex.com"
PASSWORD = "ukj3qjd.hwz2gup-QFM" # You can also hardcode this for testing

MATCHED_CSV_PATH = "matched_chat_data.csv"





def post_chat_consultation(token, chat_consultation_id, partner_id):
    url = f"{HOST}/v1/admin/chat_consultation"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "chat_consultation_id": chat_consultation_id,
        "partner_id": partner_id
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"✅ Success: {chat_consultation_id}")
    else:
        print(f"❌ Failed for {chat_consultation_id} - {response.status_code} - {response.text}")


def main():
    if not PASSWORD:
        print("❌ ERROR: Please set the CHAT_API_PASSWORD environment variable.")
        return

    token = "eyJhbGciOiJIUzM4NCJ9.eyJ1c2VyX2lkIjoiZmN1c19hZjE5ZTY3ZDFlMzY4ZDlkMWVmZmRiYzliMmE5NzBmYyIsInBhcnRuZXJfaWQiOiJmYWNjdF8wMWplcHpuZWZoemc0MXE1YnB0NGhnNm5rOSIsInRlc3RfbW9kZSI6ZmFsc2UsImV4cCI6MTc1Mzg5OTE0NH0.95DvRakuLCXKYBNWKzbpB7eWXhiCdnEmzgZ5EAYxwmydTz5R-Rb3GvB22Mj0uka3"


    df = pd.read_csv(MATCHED_CSV_PATH)
    for _, row in df.iterrows():
        chat_id = row["chat_consultation_id"]
        partner_id = row["partner_id"]
        post_chat_consultation(token, chat_id, partner_id)


if __name__ == "__main__":
    main()
