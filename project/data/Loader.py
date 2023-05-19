import pandas as pd
import sqlite3

class Loader:


    def load_initial_data(self, transformed_data):
        print("-"*60)
        print("starting loading ...")
        # Code for loading the transformed data to the destination
        conn = sqlite3.connect('bicycle_counter_factors_dataset.db')
        transformed_data.to_sql("dataset", conn, if_exists='replace')
        print("finished loading ...")
        conn.close()

    def load_incremental_data(self, transformed_data):
        # Code for loading the transformed data to the destination
        # Connect to the SQLite database
        conn = sqlite3.connect('bicycle_counter_factors_dataset.db')

        # Query the database to retrieve the newest datetime value
        query = "SELECT MAX(Datetime) FROM dataset"
        result = conn.execute(query)
        newest_datetime = result.fetchone()[0]

        print(f"newest_datetime: {newest_datetime}")
        # Filter the dataframe based on the newest datetime value
        new_rows_df = transformed_data[transformed_data.index > newest_datetime]

        # Export the new dataframe to the existing SQLite table
        if new_rows_df.empty:
            new_rows_df.to_sql('dataset', conn, if_exists='append')

        # Close the database connection
        conn.close()