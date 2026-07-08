#!/usr/bin/env python3
"""
generate_transactions.py
Generate data/transactions.parquet with 50 realistic transaction rows.

The file intentionally contains 7 columns (txn_id, amount, currency, status,
merchant, region, card_last4).  The pipeline only declares 4 of them, so the
engine demonstrates column pruning: merchant, region, and card_last4 are never
read from disk.

Usage:
    pip install pandas pyarrow
    python data/generate_transactions.py
"""

import pandas as pd
import os

rows = [
    # (txn_id,          amount,   currency, status,      merchant,          region,     card_last4)
    ("TXN-0001",  120.50,  "USD", "COMPLETED",  "Amazon",          "US-EAST",  "4242"),
    ("TXN-0002",   45.00,  "USD", "PENDING",    "Uber Eats",       "US-WEST",  "1234"),
    ("TXN-0003",  310.75,  "EUR", "COMPLETED",  "Booking.com",     "EU-WEST",  "5678"),
    ("TXN-0004",   22.99,  "GBP", "FAILED",     "Spotify",         "EU-NORTH", "9012"),
    ("TXN-0005",  550.00,  "USD", "COMPLETED",  "Apple Store",     "US-EAST",  "3456"),
    ("TXN-0006",   18.50,  "CAD", "CANCELLED",  "Tim Hortons",     "CA-EAST",  "7890"),
    ("TXN-0007",   89.99,  "USD", "COMPLETED",  "Nike",            "US-SOUTH", "2345"),
    ("TXN-0008",  200.00,  "AUD", "COMPLETED",  "JB Hi-Fi",        "AU-EAST",  "6789"),
    ("TXN-0009",   15.00,  "USD", "PENDING",    "Netflix",         "US-WEST",  "0123"),
    ("TXN-0010",  430.25,  "EUR", "COMPLETED",  "Zalando",         "EU-CENTRAL","4567"),
    ("TXN-0011",   75.60,  "USD", "COMPLETED",  "Whole Foods",     "US-NORTH", "8901"),
    ("TXN-0012",   10.00,  "GBP", "FAILED",     "Greggs",          "EU-NORTH", "2345"),
    ("TXN-0013",  999.00,  "USD", "COMPLETED",  "Best Buy",        "US-EAST",  "6789"),
    ("TXN-0014",   33.40,  "CAD", "COMPLETED",  "Shoppers",        "CA-WEST",  "0123"),
    ("TXN-0015",  650.00,  "JPY", "CANCELLED",  "Rakuten",         "AP-EAST",  "4567"),
    ("TXN-0016",   55.00,  "USD", "COMPLETED",  "Starbucks",       "US-WEST",  "8901"),
    ("TXN-0017",   42.00,  "EUR", "PENDING",    "Lidl",            "EU-SOUTH", "2345"),
    ("TXN-0018",  180.00,  "USD", "COMPLETED",  "Delta Airlines",  "US-EAST",  "6789"),
    ("TXN-0019",   25.99,  "GBP", "COMPLETED",  "ASOS",            "EU-NORTH", "0123"),
    ("TXN-0020",   70.00,  "AUD", "FAILED",     "Woolworths",      "AU-EAST",  "4567"),
    ("TXN-0021",  500.00,  "USD", "COMPLETED",  "United Airlines", "US-SOUTH", "8901"),
    ("TXN-0022",   12.50,  "CAD", "PENDING",    "Second Cup",      "CA-EAST",  "2345"),
    ("TXN-0023",  240.00,  "EUR", "COMPLETED",  "FNAC",            "EU-WEST",  "6789"),
    ("TXN-0024",   99.99,  "USD", "COMPLETED",  "Costco",          "US-WEST",  "0123"),
    ("TXN-0025",   38.75,  "GBP", "CANCELLED",  "Pret A Manger",   "EU-NORTH", "4567"),
    ("TXN-0026",  780.00,  "USD", "COMPLETED",  "Marriott",        "US-EAST",  "8901"),
    ("TXN-0027",   60.00,  "EUR", "COMPLETED",  "H&M",             "EU-CENTRAL","2345"),
    ("TXN-0028",   28.00,  "AUD", "PENDING",    "Coles",           "AU-WEST",  "6789"),
    ("TXN-0029",  115.50,  "USD", "COMPLETED",  "Target",          "US-SOUTH", "0123"),
    ("TXN-0030",   47.20,  "CAD", "FAILED",     "Sport Chek",      "CA-WEST",  "4567"),
    ("TXN-0031",  320.00,  "JPY", "COMPLETED",  "7-Eleven JP",     "AP-EAST",  "8901"),
    ("TXN-0032",   85.00,  "USD", "COMPLETED",  "REI",             "US-WEST",  "2345"),
    ("TXN-0033",   19.99,  "GBP", "PENDING",    "Amazon UK",       "EU-NORTH", "6789"),
    ("TXN-0034",  400.00,  "EUR", "COMPLETED",  "MediaMarkt",      "EU-CENTRAL","0123"),
    ("TXN-0035",   33.00,  "USD", "CANCELLED",  "Lyft",            "US-EAST",  "4567"),
    ("TXN-0036",  220.50,  "AUD", "COMPLETED",  "Qantas",          "AU-EAST",  "8901"),
    ("TXN-0037",   67.80,  "USD", "COMPLETED",  "Cheesecake Factory","US-SOUTH","2345"),
    ("TXN-0038",   14.00,  "CAD", "PENDING",    "Tim Hortons",     "CA-EAST",  "6789"),
    ("TXN-0039",  900.00,  "USD", "COMPLETED",  "Hilton",          "US-WEST",  "0123"),
    ("TXN-0040",   52.30,  "EUR", "FAILED",     "Carrefour",       "EU-SOUTH", "4567"),
    ("TXN-0041",  145.00,  "GBP", "COMPLETED",  "John Lewis",      "EU-NORTH", "8901"),
    ("TXN-0042",   30.00,  "JPY", "PENDING",    "FamilyMart",      "AP-EAST",  "2345"),
    ("TXN-0043",  275.00,  "USD", "COMPLETED",  "Southwest",       "US-EAST",  "6789"),
    ("TXN-0044",   48.00,  "AUD", "CANCELLED",  "Kmart AU",        "AU-WEST",  "0123"),
    ("TXN-0045",  135.00,  "USD", "COMPLETED",  "Nordstrom",       "US-NORTH", "4567"),
    ("TXN-0046",   22.00,  "CAD", "COMPLETED",  "Canadian Tire",   "CA-WEST",  "8901"),
    ("TXN-0047",  460.00,  "EUR", "COMPLETED",  "Lufthansa",       "EU-CENTRAL","2345"),
    ("TXN-0048",   95.50,  "USD", "FAILED",     "GrubHub",         "US-SOUTH", "6789"),
    ("TXN-0049",   78.00,  "GBP", "COMPLETED",  "Marks & Spencer", "EU-NORTH", "0123"),
    ("TXN-0050",  350.00,  "USD", "COMPLETED",  "Alaska Airlines", "US-WEST",  "4567"),
]

df = pd.DataFrame(rows, columns=[
    "txn_id", "amount", "currency", "status",
    "merchant", "region", "card_last4",
])

# Explicit types so the Parquet schema is clean and unambiguous
df["amount"] = df["amount"].astype("float64")

out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "transactions.parquet")
df.to_parquet(out_path, index=False, engine="pyarrow")

print(f"Written {len(df)} rows to {out_path}")
print(f"\nColumn schema:")
for col, dtype in df.dtypes.items():
    print(f"  {col:<12} {dtype}")

# Show filter preview
passed = df[(df["status"] == "COMPLETED") & (df["amount"] > 50.0)]
print(f"\nRows that will pass the pipeline filter (status=COMPLETED AND amount>50.0): {len(passed)}")
print(f"Rows that will be dropped by the filter: {len(df) - len(passed)}")
