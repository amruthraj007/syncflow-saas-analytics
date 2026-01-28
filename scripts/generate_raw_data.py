import duckdb
import pandas as pd
import numpy as np
import os

# Ensure the data directory exists
os.makedirs('data', exist_ok=True)

# Connect to DuckDB (This creates syncflow.db if it doesn't exist)
con = duckdb.connect('data/syncflow.db')

print("Generating synthetic SaaS data...")

# 1. Users Table: 5,000 leads
# We simulate that business domains (stripe, intercom) have larger company sizes
domains = ['gmail.com', 'outlook.com', 'stripe.com', 'intercom.io', 'hubspot.com', 'canva.com']
users = pd.DataFrame({
    'user_id': range(1, 5001),
    'email': [f"user_{i}@{np.random.choice(domains)}" for i in range(1, 5001)],
    'company_size': np.random.choice(['1-10', '11-50', '51-200', '201-500', '500+'], 5000),
    'signup_at': pd.date_range(start='2025-01-01', periods=5000, freq='20min')
})

# 2. Events Table: 30,000 interactions
# High-value events like 'teammate_invited' are rarer but indicate "PQL" status
event_types = ['login', 'project_created', 'task_added', 'teammate_invited', 'integration_connected']
event_weights = [0.4, 0.2, 0.2, 0.1, 0.1] # Probability of each event

events = pd.DataFrame({
    'event_id': range(1, 30001),
    'user_id': np.random.choice(range(1, 5001), 30000),
    'event_type': np.random.choice(event_types, 30000, p=event_weights),
    'event_time': pd.date_range(start='2025-01-01', periods=30000, freq='10min')
})

# Save to DuckDB
con.execute("CREATE OR REPLACE TABLE raw_users AS SELECT * FROM users")
con.execute("CREATE OR REPLACE TABLE raw_events AS SELECT * FROM events")

print(f"Success! Generated {len(users)} users and {len(events)} events.")
con.close()