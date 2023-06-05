import pandas as pd
import sqlite3
import numpy as np

dataset_url = 'https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV'
df = pd.read_csv(dataset_url, sep=';')
#df.to_csv("trainstops.csv", index=False)

# Drop the 'Status' column
df.drop('Status', axis=1, inplace=True)

# Replace empty cells with NaN
df.replace('', np.nan, inplace=True)

# Convert 'Laenge' and 'Breite' columns to float (german decimal separator is comma)
df['Laenge'] = df['Laenge'].str.replace(',', '.').astype(float)
df['Breite'] = df['Breite'].str.replace(',', '.').astype(float)

# Drop rows with invalid values
# Valid "Verkehr" values
valid_verkehr_values = ['FV', 'RV', 'nur DPN']
df = df[df['Verkehr'].isin(valid_verkehr_values)]
# Valid "Laeng"
df = df[(df['Laenge'] >= -90) & (df['Laenge'] <= 90)]
# Valid "Breite"
df = df[(df['Breite'] >= -90) & (df['Breite'] <= 90)]
# IFOPT values: <exactly two characters>:<any amount of numbers>:<any amount of numbers><optionally another colon followed by any amount of numbers>
df = df[df['IFOPT'].str.match(r'^[a-zA-Z]{2}:\d+:?\d*:?(\d+)?$', na=False)]

# Connect to the SQLite database
conn = sqlite3.connect('trainstops.sqlite')

# Define column types for SQLite
dtype = {
    'ID': 'BIGINT',
    'Verkehr': 'TEXT',
    'Laenge': 'FLOAT',
    'Breite': 'FLOAT',
    'IFOPT': 'TEXT'
}

# Write the modified DataFrame to the SQLite database table
df.to_sql('trainstops', conn, if_exists='replace', index=False, dtype=dtype)

# Close the database connection
conn.close()
