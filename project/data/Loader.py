import pandas as pd
import sqlite3

class Loader:


    def load_initial_data(self, transformed_data):
        # Code for loading the transformed data to the destination
        conn = sqlite3.connect('bicycle_counter_factors_dataset.db')
        transformed_data.to_sql("dataset", conn, if_exists='replace')
        conn.close()

    def load_incremental_data(self, transformed_data):
        # Code for loading the transformed data to the destination
        pass