from sqlalchemy import create_engine
import pandas as pd
import yaml
from datetime import datetime, timedelta

# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

db = config["postgres"]

engine = create_engine(
    f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
)

print("\n=== DATA VALIDATION REPORT ===\n")

# 1. Missing values
missing = pd.read_sql("""
SELECT series_id, COUNT(*) as missing_count
FROM macro_observations
WHERE value IS NULL
GROUP BY series_id
""", engine)

print("Missing Values:")
print(missing, "\n")

# 2. Latest date (freshness)
freshness = pd.read_sql("""
SELECT series_id, MAX(date) as latest_date
FROM macro_observations
GROUP BY series_id
""", engine)

print("Latest Data:")
print(freshness, "\n")

# 3. Duplicate check
duplicates = pd.read_sql("""
SELECT series_id, date, COUNT(*) as count
FROM macro_observations
GROUP BY series_id, date
HAVING COUNT(*) > 1
""", engine)

print("Duplicates:")
print(duplicates if not duplicates.empty else "No duplicates found", "\n")

# 4. Freshness rule (simple check)
today = datetime.today().date()

print("Freshness Check:")
for _, row in freshness.iterrows():
    latest = row["latest_date"]
    if latest < today - timedelta(days=120):
        print(f"WARNING: {row['series_id']} is outdated")
    else:
        print(f"{row['series_id']} is recent")

print("\n=== END REPORT ===")