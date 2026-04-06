from fredapi import Fred
import pandas as pd
from sqlalchemy import create_engine
import yaml

# load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

# setup
fred = Fred(api_key=config["fred"]["api_key"])

db = config["postgres"]
engine = create_engine(f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}")

# Define indicators
SERIES = {
    "GDP_US": {"fred_id": "GDP", "metric": "GDP", "frequency": "quarterly", "unit": "USD"},
    "CPI_US": {"fred_id": "CPIAUCSL", "metric": "Inflation", "frequency": "monthly", "unit": "Index"},
    "UNRATE_US": {"fred_id": "UNRATE", "metric": "Unemployment", "frequency": "monthly", "unit": "%"},
    "FEDFUNDS_US": {"fred_id": "FEDFUNDS", "metric": "Interest Rate", "frequency": "monthly", "unit": "%"},
    "M2_US": {"fred_id": "M2SL", "metric": "Money Supply", "frequency": "monthly", "unit": "USD"}
}

for series_id, meta in SERIES.items():
    print(f"Loading {series_id}...")

    # Extract
    data = fred.get_series(meta["fred_id"])
    df = data.reset_index()
    df.columns = ["date", "value"]
    df["series_id"] = series_id

    # Ensure datetime consistency
    df["date"] = pd.to_datetime(df["date"])

    # Insert metadata
    series_df = pd.DataFrame([{
        "series_id": series_id,
        "source": "FRED",
        "country": "USA",
        "metric": meta["metric"],
        "frequency": meta["frequency"],
        "unit": meta["unit"]
    }])

    try:
        series_df.to_sql("macro_series", engine, if_exists="append", index=False)
    except:
        pass

    # Get existing data for THIS series
    existing = pd.read_sql(
        f"SELECT series_id, date FROM macro_observations WHERE series_id = '{series_id}'",
        engine
    )

    # Fix datatype mismatch
    existing["date"] = pd.to_datetime(existing["date"])

    # Remove duplicates
    df = df.merge(existing, on=["series_id", "date"], how="left", indicator=True)
    df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

    # Remove null values
    df = df.dropna()

    # Insert new data
    if not df.empty:
        df.to_sql("macro_observations", engine, if_exists="append", index=False)

print("All macro data loaded successfully!")
