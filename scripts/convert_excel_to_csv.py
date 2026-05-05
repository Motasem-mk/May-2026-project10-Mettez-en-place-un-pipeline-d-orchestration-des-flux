import pandas as pd

files = {
    "erp.xlsx": "erp.csv",
    "web.xlsx": "web.csv",
    "liaison.xlsx": "liaison.csv",
}

for excel_file, csv_file in files.items():
    df = pd.read_excel(excel_file)
    df.to_csv(csv_file, index=False)
    print(f"{excel_file} -> {csv_file}: {len(df)} rows, {len(df.columns)} columns")