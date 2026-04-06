from fredapi import Fred
import pandas as pd
from sqlalchemy import create_engine
import yaml

# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

# FRED connection
fred = Fred(api_key=config["fred"]["api_key"])

# Get GDP data
gdp = fred.get_series("GDP")

df = gdp.reset_index()
df.columns = ["date", "value"]
df["series_id"] = "GDP_US"

# PostgreSQL connection
db = config["postgres"]
engine = create_engine(
    f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
)

# Insert into macro_series (once)
series_df = pd.DataFrame([{
    "series_id": "GDP_US",
    "source": "FRED",
    "country": "USA",
    "metric": "GDP",
    "frequency": "quarterly",
    "unit": "USD"
}])

series_df.to_sql("macro_series", engine, if_exists="append", index=False)

# Insert observations
df.to_sql("macro_observations", engine, if_exists="append", index=False)

print("GDP data loaded successfully!")