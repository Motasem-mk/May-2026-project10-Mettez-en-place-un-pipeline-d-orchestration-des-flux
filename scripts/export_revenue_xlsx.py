import pandas as pd

INPUT_FILE = "/app/data/processed/revenue_per_product.csv"
OUTPUT_FILE = "/app/data/outputs/revenue_per_product.xlsx"

df = pd.read_csv(INPUT_FILE)
total = df["revenue"].sum()

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df.to_excel(
        writer,
        index=False,
        sheet_name="Revenue by Product"
    )

    summary = pd.DataFrame({
        "Metric": ["Total Revenue"],
        "Value": [total]
    })

    summary.to_excel(
        writer,
        index=False,
        sheet_name="Summary"
    )

print("Excel revenue report created successfully.")
print(f"Total revenue: {total:.2f}")
print(f"Output file: {OUTPUT_FILE}")
