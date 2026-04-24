"""
Banking Analytics Pipeline — Data Generator
Simulates realistic N26-style neobank data
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()
random.seed(42)
np.random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data/raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2024, 12, 31)
N_USERS    = 5_000

COUNTRIES   = ["DE", "AT", "ES", "FR", "IT", "NL", "BE", "PT"]
COUNTRY_W   = [0.40, 0.12, 0.12, 0.10, 0.08, 0.07, 0.06, 0.05]
PLAN_TYPES  = ["free", "smart", "metal"]
PLAN_W      = [0.65, 0.25, 0.10]
DEVICES     = ["ios", "android"]
KYC_STATUS  = ["verified", "pending", "failed"]
KYC_W       = [0.88, 0.08, 0.04]

TX_TYPES    = ["payment", "transfer", "atm"]
TX_W        = [0.60, 0.30, 0.10]
TX_STATUS   = ["completed", "failed", "pending"]
TX_STATUS_W = [0.92, 0.05, 0.03]
MCC         = ["groceries","restaurants","transport","entertainment",
                "shopping","travel","health","utilities","other"]

EVENT_TYPES = ["app_open","card_activated","first_transfer",
               "notification_clicked","settings_changed","support_opened"]

CARD_TYPES   = ["virtual", "physical"]
CARD_STATUS  = ["active", "blocked", "cancelled"]
CARD_STATUS_W= [0.85, 0.10, 0.05]


def rand_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


# ── USERS ──────────────────────────────────────────────────────────────────────
def generate_users(n=N_USERS):
    rows = []
    for i in range(n):
        signup = rand_date(START_DATE, END_DATE - timedelta(days=30))
        rows.append({
            "user_id":      f"u_{i+1:05d}",
            "created_at":   signup.isoformat(),
            "country":      np.random.choice(COUNTRIES, p=COUNTRY_W),
            "device_type":  random.choice(DEVICES),
            "kyc_status":   np.random.choice(KYC_STATUS, p=KYC_W),
            "plan_type":    np.random.choice(PLAN_TYPES, p=PLAN_W),
            "email":        fake.email(),
            "age_bucket":   np.random.choice(["18-24","25-34","35-44","45+"],
                                              p=[0.25,0.40,0.22,0.13]),
        })
    return pd.DataFrame(rows)


# ── TRANSACTIONS ───────────────────────────────────────────────────────────────
def generate_transactions(users_df):
    rows = []
    tx_id = 1
    verified = users_df[users_df["kyc_status"] == "verified"]

    for _, user in verified.iterrows():
        signup = datetime.fromisoformat(user["created_at"])
        # active users make more transactions
        n_tx = np.random.negative_binomial(5, 0.15)  # heavy-tailed
        n_tx = max(1, min(n_tx, 300))

        for _ in range(n_tx):
            tx_date = rand_date(signup, END_DATE)
            tx_type = np.random.choice(TX_TYPES, p=TX_W)

            if tx_type == "atm":
                amount = round(random.uniform(20, 500), 2)
            elif tx_type == "transfer":
                amount = round(random.uniform(10, 2000), 2)
            else:
                amount = round(np.random.lognormal(3.5, 1.0), 2)
                amount = min(amount, 5000)

            rows.append({
                "transaction_id":    f"tx_{tx_id:07d}",
                "user_id":           user["user_id"],
                "created_at":        tx_date.isoformat(),
                "amount":            amount,
                "currency":          "EUR",
                "type":              tx_type,
                "merchant_category": random.choice(MCC) if tx_type == "payment" else None,
                "status":            np.random.choice(TX_STATUS, p=TX_STATUS_W),
                "country":           user["country"],
            })
            tx_id += 1

    return pd.DataFrame(rows)


# ── APP EVENTS ─────────────────────────────────────────────────────────────────
def generate_app_events(users_df):
    rows = []
    ev_id = 1

    for _, user in users_df.iterrows():
        signup = datetime.fromisoformat(user["created_at"])

        # onboarding events (all users)
        for event in ["app_open", "card_activated"]:
            offset = random.randint(0, 3)
            rows.append({
                "event_id":   f"ev_{ev_id:07d}",
                "user_id":    user["user_id"],
                "created_at": (signup + timedelta(days=offset)).isoformat(),
                "event_type": event,
                "platform":   user["device_type"],
            })
            ev_id += 1

        # first transfer — only verified users
        if user["kyc_status"] == "verified":
            rows.append({
                "event_id":   f"ev_{ev_id:07d}",
                "user_id":    user["user_id"],
                "created_at": (signup + timedelta(days=random.randint(1, 7))).isoformat(),
                "event_type": "first_transfer",
                "platform":   user["device_type"],
            })
            ev_id += 1

        # recurring engagement events
        n_events = np.random.poisson(15)
        for _ in range(n_events):
            rows.append({
                "event_id":   f"ev_{ev_id:07d}",
                "user_id":    user["user_id"],
                "created_at": rand_date(signup, END_DATE).isoformat(),
                "event_type": np.random.choice(
                    EVENT_TYPES[2:], p=[0.35, 0.30, 0.20, 0.15]
                ),
                "platform":   user["device_type"],
            })
            ev_id += 1

    return pd.DataFrame(rows)


# ── CARDS ──────────────────────────────────────────────────────────────────────
def generate_cards(users_df):
    rows = []
    card_id = 1

    for _, user in users_df.iterrows():
        signup = datetime.fromisoformat(user["created_at"])
        n_cards = 1 if user["plan_type"] == "free" else random.randint(1, 2)

        for i in range(n_cards):
            rows.append({
                "card_id":    f"card_{card_id:06d}",
                "user_id":    user["user_id"],
                "created_at": (signup + timedelta(days=random.randint(0, 5))).isoformat(),
                "card_type":  "virtual" if i == 0 else "physical",
                "status":     np.random.choice(CARD_STATUS, p=CARD_STATUS_W),
                "plan_type":  user["plan_type"],
            })
            card_id += 1

    return pd.DataFrame(rows)


# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating users...")
    users = generate_users()
    users.to_csv(f"{OUTPUT_DIR}/users.csv", index=False)
    print(f"  {len(users):,} users saved")

    print("Generating transactions...")
    transactions = generate_transactions(users)
    transactions.to_csv(f"{OUTPUT_DIR}/transactions.csv", index=False)
    print(f"  {len(transactions):,} transactions saved")

    print("Generating app events...")
    events = generate_app_events(users)
    events.to_csv(f"{OUTPUT_DIR}/app_events.csv", index=False)
    print(f"  {len(events):,} events saved")

    print("Generating cards...")
    cards = generate_cards(users)
    cards.to_csv(f"{OUTPUT_DIR}/cards.csv", index=False)
    print(f"  {len(cards):,} cards saved")

    print("\nDone! Files saved to data/raw/")
