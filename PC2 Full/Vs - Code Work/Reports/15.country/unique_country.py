import pandas as pd
import pycountry

input_csv = r"D:\Veto Logs Backup\Reports\unique_country.csv"
output_csv = r"D:\Veto Logs Backup\Reports\country_unique_with_names.csv"

df = pd.read_csv(input_csv)

def country_name(code):
    code = str(code).strip().upper()
    try:
        c = pycountry.countries.get(alpha_2=code)
        return c.name if c else "Unknown"
    except Exception:
        return "Unknown"

df["country_name"] = df["value"].map(country_name)

df = df[["value", "country_name", "count", "% of rows"]]

df.to_csv(output_csv, index=False)
print(df.to_string(index=False))
print(f"\nSaved: {output_csv}")