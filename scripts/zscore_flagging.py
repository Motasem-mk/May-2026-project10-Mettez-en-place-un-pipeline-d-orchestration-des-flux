import pandas as pd

# INPUT_FILE = "merged_with_revenue.csv"
# OUTPUT_FILE = "premium_flagged.csv"
INPUT_FILE = "/app/data/processed/merged_with_revenue.csv"
OUTPUT_FILE = "/app/data/processed/premium_flagged.csv"
EXPECTED_PREMIUM_COUNT = 30

df = pd.read_csv(INPUT_FILE)

mean_price = df["price"].mean()
std_price = df["price"].std()

df["z_score"] = (df["price"] - mean_price) / std_price
df["is_vintage"] = df["z_score"] > 2

premium_count = int(df["is_vintage"].sum())

assert premium_count == EXPECTED_PREMIUM_COUNT, (
    f"Z-score test failed: expected {EXPECTED_PREMIUM_COUNT} premium wines, "
    f"got {premium_count}"
)

df.to_csv(OUTPUT_FILE, index=False)

print("Z-score flagging completed.")
print(f"Mean price: {mean_price}")
print(f"Standard deviation: {std_price}")
print(f"Premium wines detected: {premium_count}")
print("Z-score test passed.")
print(f"Output saved to: {OUTPUT_FILE}")