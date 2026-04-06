import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import yaml

# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

db = config["postgres"]

engine = create_engine(
    f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
)

# Load pivoted data
df = pd.read_sql("SELECT * FROM macro_pivot", engine)

# Convert date
df["date"] = pd.to_datetime(df["date"])

# Plot unemployment
plt.figure()
plt.plot(df["date"], df["unemployment"])
plt.title("US Unemployment Rate Over Time")
plt.xlabel("Date")
plt.ylabel("Unemployment (%)")
plt.show()

# Plot interest rate vs inflation
plt.figure()
plt.plot(df["date"], df["interest_rate"], label="Interest Rate")
plt.plot(df["date"], df["cpi"], label="Inflation (CPI)")
plt.legend()
plt.title("Interest Rate vs Inflation")
plt.show()