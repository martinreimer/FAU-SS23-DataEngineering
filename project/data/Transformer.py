import pandas as pd
from io import StringIO
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
        print(extracted_data)

    #endregion
        
    def transform_initial_data(self, extracted_data):
        # Code for transforming the extracted data
        #print(extracted_data["source1"]["data"])
        source1_t_df = self.__transform_initial_data_source1(extracted_data["source1"])

        source2_t_df = self.__transform_initial_data_source1(extracted_data["source2"])

