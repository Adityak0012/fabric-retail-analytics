"""Shared settings for every generator/simulator script.

Sizes are deliberately small so everything runs comfortably on a
Fabric trial capacity. Scale them up once the pipeline works.
"""
from datetime import date

SEED = 42

# ---- volumes -------------------------------------------------------------
N_CUSTOMERS = 5_000
N_PRODUCTS = 400
HISTORY_START = date(2025, 3, 1)      # first online order
HISTORY_END = date(2026, 9, 24)       # last historical order (yesterday-ish)
INVENTORY_DAYS = 30                   # daily inventory snapshot files
AVG_ORDERS_PER_ACTIVE_MONTH = 0.9     # per customer, before churn

# ---- geography / currency -------------------------------------------------
# Each country maps to the currency its stores and online orders use.
# All currencies are available from the Frankfurter FX API.
COUNTRIES = {
    "India":          {"currency": "INR", "weight": 0.60,
                       "cities": [("Mumbai", "Maharashtra"), ("Pune", "Maharashtra"),
                                  ("Bengaluru", "Karnataka"), ("Delhi", "Delhi"),
                                  ("Hyderabad", "Telangana"), ("Chennai", "Tamil Nadu")]},
    "Singapore":      {"currency": "SGD", "weight": 0.10,
                       "cities": [("Singapore", "Central")]},
    "United Kingdom": {"currency": "GBP", "weight": 0.12,
                       "cities": [("London", "England"), ("Manchester", "England")]},
    "Germany":        {"currency": "EUR", "weight": 0.08,
                       "cities": [("Berlin", "Berlin"), ("Munich", "Bavaria")]},
    "United States":  {"currency": "USD", "weight": 0.10,
                       "cities": [("New York", "NY"), ("Austin", "TX"), ("Seattle", "WA")]},
}

# Rough price multiplier so a product "costs" similar real value everywhere
# (prices are stored in the product's base currency INR; local prices are derived).
INR_PER_UNIT = {"INR": 1.0, "SGD": 64.0, "GBP": 112.0, "EUR": 95.0, "USD": 88.0}

CATEGORIES = {
    "Electronics":   (1_500, 60_000),
    "Home & Kitchen": (300, 12_000),
    "Apparel":       (400, 6_000),
    "Beauty":        (150, 3_500),
    "Grocery":       (40, 1_200),
    "Sports":        (500, 15_000),
    "Books":         (150, 1_500),
    "Toys":          (250, 5_000),
}

LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
PAYMENT_METHODS = ["UPI", "Card", "Cash", "Wallet", "NetBanking"]
ORDER_CHANNELS = ["web", "mobile_app"]

OUTPUT_DIR = "output"
