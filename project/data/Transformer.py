import pandas as pd
from io import StringIO
from pytz import timezone
# Adjust display settings to show all columns
pd.set_option('display.max_columns', None)

class Transformer:
    #region Source1
    def __transform_initial_data_source1(self, extracted_data):
        data = extracted_data["data"]
        addresses_dfs = {}
        address_ids = extracted_data["data"].keys()
        for address_id in address_ids:
            monthly_files = data[address_id].keys()
            
            #init list of monthly dfs
            monthly_dfs = []
            for monthly_file in monthly_files:
                csv_data = data[address_id][monthly_file]["file"]
                df = pd.read_csv(StringIO(csv_data))
                monthly_dfs.append(df)
                
            #concat monthly dfs into single df
            address_df = pd.concat(monthly_dfs, ignore_index=True)
            address_df['datetime'] = pd.to_datetime(address_df['Datetime'])
            address_df.set_index('datetime', inplace=True)
            addresses_dfs[address_id] = address_df


        final_df = pd.concat(addresses_dfs.values(), axis=1, keys=addresses_dfs.keys())
        
        #drop address ids of column
        final_df.columns = final_df.columns.droplevel(0)

        #save
        final_df.to_csv('source1_df.csv', index=False)
        return final_df

    #endregion
    #region source2

    def __transform_initial_data_source2(self, extracted_data):
        CSV_COLUMN_NAMES = ["date", "hour_utc", "temp", "dwpt", "rhum", "prcp", "snow", "wdir", "wspd", "wpgt", "pres", "tsun", "condition_code"]
        years_dfs = []


        years = list(extracted_data.keys())
        #print(f"years: {years}")
        for year in years:
            yearly_csv = extracted_data[year]
            #check if data available
            if yearly_csv != "":
                yearly_df = pd.read_csv(StringIO(yearly_csv), names=CSV_COLUMN_NAMES)
                #print(yearly_df.head(2))
                # Combine date and hour columns into a single string column
                yearly_df['datetime_str'] = yearly_df['date'] + ' ' + yearly_df['hour_utc'].astype(str) + ':00:00'

                # Convert the combined column to datetime
                #insert new column at first position
                yearly_df.insert(1, "datetime", value=None)
                yearly_df['datetime'] = pd.to_datetime(yearly_df['datetime_str'], format='%Y-%m-%d %H:%M:%S')

                # Drop the intermediate columns if needed
                yearly_df = yearly_df.drop(['date', 'hour_utc', 'datetime_str'], axis=1)

                # Convert datetime to UTC+2
                '''
                timezone_name = 'Europe/Berlin'
                local_timezone = timezone(timezone_name)
                yearly_df['datetime'] = yearly_df['datetime'].dt.tz_localize('UTC').dt.tz_convert(local_timezone)
                '''
                # Set datetime column as index
                yearly_df.set_index('datetime', inplace=True, drop=False)
                years_dfs.append(yearly_df)
                

        final_df = pd.concat(years_dfs, ignore_index=True)
        final_df.set_index('datetime', inplace=True, drop=False)
        final_df.to_csv('source2_1_df.csv', index=False)
        return final_df

    #endregion
        
    #region source3

    def __transform_initial_data_source3(self, extracted_data):
        years = list(extracted_data.keys())
        holidays, dates = [], []
        for year in years:
            holidays_years = list(extracted_data[year].keys())
            for holiday in holidays_years:
                holidays.append(holiday)
                dates.append(extracted_data[year][holiday]['datum'])
        final_df = pd.DataFrame({'date': dates, 'holiday': holidays})
        #transform date string to datetime format
        final_df['date'] = pd.to_datetime(final_df['date'])
        final_df.set_index('date', inplace=True, drop=False)
        
        #print(final_df.head(2))
        final_df.to_csv('source3_df.csv', index=False)
        return final_df

    #endregion


    def transform_initial_data(self, extracted_data):
        print("starting initial transformation ...")
        # Code for transforming the extracted data
        #print(extracted_data["source1"]["data"])
        print("-"*60)
        print("transforming source 1 ...")
        source1_df = self.__transform_initial_data_source1(extracted_data["source1"])
        print("-"*60)
        print("transforming source 2 ...")
        source2_df = self.__transform_initial_data_source2(extracted_data["source2"])
        print("-"*60)
        print("transforming source 3 ...")       
        source3_df = self.__transform_initial_data_source3(extracted_data["source3"])

        print("-"*60)
        print("merging sources...")
        #merge all 3 sources together
        # Assuming you have three dataframes named DF1, DF2, and DF3

        # Assuming your DataFrame is named 'df' with a datetime index
        # Resample the DataFrame to have data points every 15 minutes
        print(f"source2_df.index.dtype: {source2_df.index.dtype} - {source2_df.index.name}")
        '''
        source2_df_resampled = source2_df.resample('15T').asfreq()

        # Forward fill the missing values
        source2_df = source2_df_resampled.ffill()
        source2_df.to_csv('source2_1_df.csv', index=False)
        '''




        # Merge DF1 and DF2 based on the index, filling up missing values from DF2
        merged_df = source1_df.merge(source2_df, how='left', left_index=True, right_index=True)
        # If you want to forward fill missing values, you can use ffill() function
        merged_df = merged_df.ffill()
        # Merge the merged_df and DF3 based on the index, filling up missing values from DF3
        merged_df = merged_df.merge(source3_df, how='left', left_index=True, right_index=True)
        
        #print(merged_df.head(2))

        # Remove the duplicated columns
        merged_df = merged_df.reset_index(drop=True)
        merged_df = merged_df.drop('Datetime', axis=1)

        merged_df.to_csv('source_m_df.csv', index=False)

        return merged_df


