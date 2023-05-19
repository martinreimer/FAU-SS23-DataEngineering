import pandas as pd
from io import StringIO
from datetime import timedelta
# Adjust display settings to show all columns
pd.set_option('display.max_columns', None)

class Transformer:
    #region Source1
    def __transform_data_source1(self, extracted_data):
        """
        Transform the data for source 1.
        :param extracted_data (dict) - extracted data containing monthly files for each address.
        :return pandas.DataFrame - transformed dataframe.

        """
        # Dictionary to store address-wise dataframes
        addresses_dfs = {}
        address_ids = extracted_data.keys()
        # Iterate over each address ID
        for address_id in address_ids:
            # Get the list of monthly files for the current address
            monthly_files = extracted_data[address_id].keys()
            # Initialize a list to store monthly dataframes
            monthly_dfs = []
            # Iterate over each monthly file
            for monthly_file in monthly_files:
                csv_data = extracted_data[address_id][monthly_file]["file"]
                df = pd.read_csv(StringIO(csv_data))
                monthly_dfs.append(df)
                
            # Concatenate the monthly dataframes into a single dataframe for the address
            address_df = pd.concat(monthly_dfs, ignore_index=True)
            # Convert the 'Datetime' column to datetime format
            address_df['datetime'] = pd.to_datetime(address_df['Datetime'])
            address_df.set_index('datetime', inplace=True)
            # Store the address dataframe in the dictionary using the address ID as the key
            addresses_dfs[address_id] = address_df

        # Concatenate the address-wise dictionares into a final dataframe, with address IDs as column keys
        final_df = pd.concat(addresses_dfs.values(), axis=1, keys=addresses_dfs.keys())
        #drop address ids from column
        final_df.columns = final_df.columns.droplevel(0)
        # Optionaly: Save the final dataframe to a CSV file
        #final_df.to_csv('source1_df.csv', index=False)
        return final_df

    
    #endregion
    #region source2
    def __transform_data_source2(self, extracted_data):
        """
        Transform the data for source 2.

        :param extracted_data (dict): Extracted data containing yearly CSV data for each year.
        :return pandas.DataFrame: Transformed dataframe.

        """
        CSV_COLUMN_NAMES = ["date", "hour_utc", "temp", "dwpt", "rhum", "prcp", "snow", "wdir", "wspd", "wpgt", "pres", "tsun", "condition_code"]
        years_dfs = []

        years = list(extracted_data.keys())
        # print(f"years: {years}")
        for year in years:
            yearly_csv = extracted_data[year]
            # check if data available
            if yearly_csv != "":
                yearly_df = pd.read_csv(StringIO(yearly_csv), names=CSV_COLUMN_NAMES)

                # Combine date and hour columns into a single string column
                yearly_df['datetime_str'] = yearly_df['date'] + ' ' + yearly_df['hour_utc'].astype(str) + ':00:00'

                # Convert the combined column to datetime
                # insert new column at first position
                yearly_df.insert(1, "datetime", value=None)
                yearly_df['datetime'] = pd.to_datetime(yearly_df['datetime_str'], format='%Y-%m-%d %H:%M:%S')

                # Drop the intermediate columns
                yearly_df = yearly_df.drop(['date', 'hour_utc', 'datetime_str'], axis=1)

                # Convert datetime to UTC+2 by adding two hours to each datetime value
                yearly_df['datetime'] += timedelta(hours=2)

                # Set datetime column as index
                yearly_df.set_index('datetime', inplace=True, drop=False)
                years_dfs.append(yearly_df)

        final_df = pd.concat(years_dfs, ignore_index=True)
        final_df.set_index('datetime', inplace=True, drop=False)
        # Optionaly: Save the final dataframe to a CSV file
        #final_df.to_csv('source2_1_df.csv', index=False)
        return final_df


    #endregion
        
    #region source3
    def __transform_data_source3(self, extracted_data):
        """
        Transform the initial data for source 3.

        :param extracted_data (dict): Extracted data containing holidays for each year.
        :return pandas.DataFrame: Transformed dataframe.

        """
        years = list(extracted_data.keys())
        holidays, dates = [], []
        for year in years:
            holidays_years = list(extracted_data[year].keys())
            for holiday in holidays_years:
                holidays.append(holiday)
                dates.append(extracted_data[year][holiday]['datum'])
        final_df = pd.DataFrame({'date': dates, 'holiday': holidays})
        
        # Transform date string to datetime format
        final_df['date'] = pd.to_datetime(final_df['date'])
        final_df.set_index('date', inplace=True, drop=False)
        
        # Optionaly: Save the final dataframe to a CSV file
        #final_df.to_csv('source3_df.csv', index=False)
        return final_df


    #endregion


    def transform_data(self, extracted_data):
        """
        Transform the extracted data.

        :param extracted_data (dict): The extracted data to be transformed.
        :return pandas.DataFrame: The transformed dataframe.

        """
        # Perform transformations for each data source
        print("-"*60)
        print("starting transformation ...")
        print("transforming source 1 ...")
        source1_df = self.__transform_data_source1(extracted_data["source1"])
        print("transforming source 2 ...")
        source2_df = self.__transform_data_source2(extracted_data["source2"])
        print("transforming source 3 ...")       
        source3_df = self.__transform_data_source3(extracted_data["source3"])
        
        
        #merge all 3 sources together
        print("merging sources...")
        # Merge source1 and source1 based on the index
        merged_df = source1_df.merge(source2_df, how='left', left_index=True, right_index=True)
        # Apply forward-fill only to columns from source2
        source2_df_columns = source2_df.columns
        merged_df[source2_df_columns] = merged_df[source2_df_columns].ffill()
        # Merge the merged_df and source3 based on the index,
        merged_df = merged_df.merge(source3_df, how='left', left_index=True, right_index=True)
        # Create the "is_holiday" column based on the values in source2's "holiday" column
        merged_df['is_holiday'] = merged_df['holiday'].notna()
        # Drop the unnecessary columns: holiday name and date
        merged_df.drop(['date', 'holiday'], axis=1, inplace=True)


        # Remove the duplicated "Datetime" columns, problem: index also Datetime
        # -> droppping all datetime columns and inserting one again
        datetime_col = merged_df["Datetime"]
        merged_df = merged_df.drop("Datetime", axis=1)
        merged_df = merged_df.drop("datetime", axis=1)
        merged_df.insert(0, "Datetime", value=None)
        merged_df["Datetime"]  = datetime_col.iloc[:, 0]
        # Set the remaining columns as the index
        merged_df.set_index("Datetime", inplace=True, drop=True)
        #Optionally save as csv
        #merged_df.to_csv('source_m_df.csv', index=False)
        print("finished transformation")

        return merged_df
        