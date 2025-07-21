import pandas as pd
import json
import csv

# Load CSVs
failed_df = pd.read_csv(
    "/Users/varsha/Downloads/failed.csv"
)

chat_df = pd.read_csv("/Users/varsha/Downloads/chat.csv")

# Extract customer IDs from failed.csv
customer_ids = []

for message in failed_df["message"]:
    try:
        data = json.loads(message)
        customer = data.get("flex_internal_events", {}).get("object", {}).get("consultation", {}).get("customer")
        if customer:
            customer_ids.append(customer)
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"Skipping row due to parsing error: {e}")

# Filter chat.csv by matching customer_id
matched_rows = chat_df[chat_df["customer_id"].isin(customer_ids)]

# Select relevant columns
output_df = matched_rows[["customer_id", "chat_consultation_id", "partner_id"]]

# Output to CSV
output_df.to_csv("matched_chat_data.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

print("✅ Output written to matched_chat_data.csv")
