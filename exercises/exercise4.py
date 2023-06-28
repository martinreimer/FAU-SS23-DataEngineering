import zipfile
import urllib.request
import pandas as pd
import sqlite3

# Step 1: Download and unzip data
#region Step 1
zip_file_url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
zip_file_path = "mowesta-dataset.zip"
data_csv_filename = "data.csv"

urllib.request.urlretrieve(zip_file_url, zip_file_path)

with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extract(data_csv_filename)
#endregion

# Step 2: Reshape data
#region Step 2
columns_to_keep = [
    "Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)",
    "Batterietemperatur in °C", "Geraet aktiv"
]
new_column_names = {
    "Temperatur in °C (DWD)": "Temperatur",
    "Batterietemperatur in °C": "Batterietemperatur"
}

df = pd.read_csv(data_csv_filename, delimiter=';', index_col=False, usecols=columns_to_keep)
df.rename(columns=new_column_names, inplace=True)
#endregion

# Step 3: Transform data
#region Step 3
# Celsius to Fahrenheit
df["Temperatur"] = df["Temperatur"].astype(str).apply(lambda x: float(x.replace(',', '.')))
df["Batterietemperatur"] = df["Batterietemperatur"].astype(str).apply(lambda x: float(x.replace(',', '.')))

# Celsius to Fahrenheit
df["Temperatur"] = (df["Temperatur"] * 9/5) + 32
df["Batterietemperatur"] = (df["Batterietemperatur"] * 9/5) + 32
#endregion

# Step 4: Validate data
#region Step 4
# Fahrenheit Temperature Validation
valid_temperature_range = (-459.67, 212)
temperature_valid = df["Temperatur"].between(*valid_temperature_range)
df = df[temperature_valid]

# Batterietemperatur Validation
battery_temperature_valid = df["Batterietemperatur"].between(*valid_temperature_range)
df = df[battery_temperature_valid]

# Data Type Validation
df = df.astype({
    "Geraet": int,
    "Monat": int,
    "Geraet aktiv": str
})
#endregion

# Step 5: Write data to SQLite database
#region Step 5
database_name = "temperatures.sqlite"
table_name = "temperatures"

connection = sqlite3.connect(database_name)
df.to_sql(table_name, connection, if_exists='replace', index=False)
connection.close()
#endregion
