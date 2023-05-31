#!/bin/bash

# Execute initial ETL test
python data/data_engineering.py initial_etl

# Validate output files from initial ETL
if [ -f data/initial_run.csv ] && [ -f data/bicycle_counter_factors_dataset.db ]; then
    echo "Output files from initial ETL exist. Test passed."
else
    echo "Output files from initial ETL do not exist. Test failed."
fi

# Execute incremental ETL test
python data/data_engineering.py incremental_etl

# Validate output files from incremental ETL
if [ -f data/incremental_run.csv ] && [ -f data/bicycle_counter_factors_dataset.db ]; then
    echo "Output files from incremental ETL exist. Test passed."
else
    echo "Output files from incremental ETL do not exist. Test failed."
fi