import requests
import config #import config.py with secret tokens
from datetime import datetime
import io
import pandas as pd
import json
from tqdm import tqdm, trange
import time
import gzip
import csv
from helper_methods import get_current_date


class Extractor:
    def __init__(self) -> None:
        #Source1: Bicycle Traffic Source (Github)
        self.source1_params = {
            'REPO_OWNER': "od-ms",
            'REPO_NAME': "radverkehr-zaehlstellen",
            'API_TOKEN': config.github_api_token,            
        }
        #Source2: Weather Data Soure (Metostat)
        self.source2_params = {
            'REPO_OWNER': "od-ms",
            'REPO_NAME': "radverkehr-zaehlstellen",
            'STATION_CODE': "R8WPP"
        }
        #Source3: Holidays Data Soure (Feiertage-API)
        self.source3_params = {
            'BUNDESLAND': "nw",
            'BASE_PATH': "https://feiertage-api.de/api/",
        }

    #region source1: Bicycle Traffic Source (Github)
    def __get_source1_file_name_from_datetime(self, time: datetime) -> str:
        """
        generate csv-filename in necessary format for input datetime

        :param time: datetime object
        :return: csv-filename, e.g. "2023-05-csv"
        """ 
        filling_zero = ""
        if time.month < 10: filling_zero = "0"
        return f"{str(time.year)}-{filling_zero + str(time.month)}.csv"


    def __get_source1_repo_contents(self, path):
        """
        get all filenames in given github repo path 

        :param path: str
        :return: list of files
        """ 
        contents_url = f"https://api.github.com/repos/{self.source1_params['REPO_OWNER']}/{self.source1_params['REPO_NAME']}/contents/{path}"
        contents_url = contents_url.strip()
        headers = {
            "Authorization": f"Bearer {self.source1_params['API_TOKEN']}"
        }

        response = requests.get(contents_url, headers=headers)
        response.raise_for_status()  # Raise an exception if the request was unsuccessful
        return response.json()


    def __get_source1_file(self, path, is_path_url=False):
        """
        get specific file (csv/json) from github repo

        :param path: str
        :param is_path_url: bool {False: path is subpath, True: path is full url}
        :return: string (csv/json)
        """ 
        #get csv file as bytes
        if(is_path_url):
            download_url = path
        else:
            download_url = f"https://raw.githubusercontent.com/{self.source1_params['REPO_OWNER']}/{self.source1_params['REPO_NAME']}/{path}"
        headers = {
            "Authorization": f"Bearer {self.source1_params['API_TOKEN']}"
        }
        file_response = requests.get(download_url, headers=headers)
        file_response.raise_for_status()
        #decode bytes to utf-8
        file_content = file_response.content.decode('utf-8')
        return file_content
    

    def __get_initial_address_ids(self, source1_metadata_list):
        """
        extract address ids from source1 metadata output

        :param source1_metadata_list: list of addresses
        :return: list 
        """ 
        address_ids = []
        for address in source1_metadata_list:
            address_ids.append(address["directory"])
        return address_ids


    def __extract_initial_data_from_source1(self):
        """
        extracting data from the data source 1 for initial ETL

        :return: dictionary with keys "metadata" & "data"
        """ 
        #init output dict
        output_source1_dict = {}


        #get initial metadata of all counters
        metadata_path = "/main/site_min.json"
        source1_metadata = self.__get_source1_file(metadata_path)
        #transform string to py object -> list of counters
        source1_metadata_list = json.loads(source1_metadata)
        #save metadata into dict
        output_source1_dict["metadata"] = source1_metadata_list


        #get address ids in order to get data for all addresses/counters
        address_ids_list = self.__get_initial_address_ids(source1_metadata_list=source1_metadata_list)
        print(f"address_ids_list: {address_ids_list}")


        #get all files for all addresses
        #init data dictionary
        output_source1_dict["data"] = {}
        #create progressbar
        max_iter1 = len(address_ids_list)
        print("Downloading all files for all addresses")
        pbar = tqdm(total=max_iter1)
        #go through all address ids
        for address_id in address_ids_list:
            #print(f"address_id: {address_id}")
            #init dict for this address_id
            output_source1_dict["data"][address_id] = {}
            path = f"{address_id}"
            #files per month in repo for this address id
            files_per_month_for_address_id = self.__get_source1_repo_contents(path=path)
            
            for file_per_month in files_per_month_for_address_id:
                #get name and download_url to this file
                file_name = file_per_month["name"]

                download_url = file_per_month["download_url"]

                #get file for this month: csv as string
                file_string = self.__get_source1_file(path=download_url, is_path_url=True)

                #save file in dictionary
                output_source1_dict["data"][address_id][file_name] = {}
                output_source1_dict["data"][address_id][file_name]["file"] = file_string
                
            time.sleep(0.1)
            pbar.update(1)
        pbar.close()
        return output_source1_dict
    

    def __extract_incremental_data_from_source1(self):
        """
        extracting data from the data source 1 for incremental ETL

        :return: dictionary only with data (no metadata)
        """ 
        #init output dict
        output_source1_dict = {}

        # --> to adjust

        #get initial metadata of all counters
        metadata_path = "/main/site_min.json"
        source1_metadata = self.__get_source1_file(metadata_path)
        #transform string to py object -> list of counters
        source1_metadata_list = json.loads(source1_metadata)


        #get address ids in order to get data for all addresses/counters
        address_ids_list = self.__get_initial_address_ids(source1_metadata_list=source1_metadata_list)
        print(f"address_ids_list: {address_ids_list}")


        #get current file for all addresses
        #create progressbar
        max_iter1 = len(address_ids_list)
        print("Downloading all files for all addresses")
        pbar = tqdm(total=max_iter1)
        #go through all address ids
        for address_id in address_ids_list:
            #print(f"address_id: {address_id}")
            #init dict for this address_id
            output_source1_dict[address_id] = {}
            path = f"{address_id}"

            #get current months files
            #get current filename
            datetime = get_current_date()
            file_name = self.__get_source1_file_name_from_datetime(time=datetime)
            path = f"main/{address_id}/{file_name}"

            #get file for this month: csv as string
            file_string = self.__get_source1_file(path=path)

            #save file in dictionary
            output_source1_dict[address_id][file_name] = {}
            output_source1_dict[address_id][file_name]["file"] = file_string
            
            time.sleep(0.1)
            pbar.update(1)
        pbar.close()
        return output_source1_dict
    #endregion


    #region source2: Meteostat
    '''
    def __get_source2_weather_station(self):
        """
        get all weather stations

        :param source1_metadata_list: list of addresses
        """ 
        url = "https://bulk.meteostat.net/v2/stations/full.json.gz"
        response = requests.get(url, stream=True)

        # Check if the request was successful
        if response.status_code == 200:
            filename = "full.json.gz"

            # Save the response content to a file
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)

        with gzip.open(filename, 'rt') as f:
            data = json.load(f)
                
            # Process the JSON data
            for station in data:
                # Access individual station data
                print(f"{station['id']}, {station['name']}")
    '''


    def __get_source2_annual_data(self, year):
        """
        get annual data dump for weather data 
        :param year
        :return: str in csv-format 
        """ 
        STATION_CODE = self.source2_params['STATION_CODE']
        url = f"https://bulk.meteostat.net/v2/hourly/{year}/{STATION_CODE}.csv.gz"
        response = requests.get(url, stream=True)
        csv_string = ""
        # Check if the request was successful
        if response.status_code == 200:
            # Read the response content into a BytesIO buffer
            buffer = io.BytesIO(response.content)

            # Open the gzip file
            with gzip.open(buffer, 'rt') as f:
                # Read the CSV data into a string
                csv_string = f.read()
        return csv_string


    def __extract_initial_data_from_source2(self, start_year, current_year):
        """
        extracting data from the data source 2 for initial ETL

        :param start_year
        :param current_year
        :return: dictionary with key for each year
        """ 
        output_source2_dict = {}

        #loop through all needed years
        for year in range(start_year, current_year + 1):
            print(year)
            output_for_year = self.__get_source2_annual_data(year=year)
            #save into dictionary
            output_source2_dict[year] = output_for_year
            
            if(start_year == current_year):
                break
        
        return output_source2_dict
        



    def __extract_incremental_data_from_source2(self, current_year):
        """
        extracting data from the data source 2 for incremental ETL

        :param current_year
        :return: dictionary with key for current year
        """ 
        output_source2_dict = {}
        #loop through all needed years
        output_for_year = self.__get_source2_annual_data(year=current_year)
        #save into dictionary
        output_source2_dict[current_year] = output_for_year
        return output_source2_dict

    #endregion

    #region source3: Feiertage-API


    def __extract_initial_data_from_source3(self, start_year: int, current_year: int):
        """
        extracting holidays data from  start_time to current time for initial ETL

        :param start_year
        :param current_year
        :return: list of dictionaries with all holidays with keys for each year
        """     
        output_source3_dict = {}

        for year in range(start_year, current_year + 1):
            params = {
                "jahr": year,
                "nur_land": self.source3_params["BUNDESLAND"]
            }
            response = requests.get(self.source3_params["BASE_PATH"], params=params)
            holidays_in_year = response.json()
            output_source3_dict[year] = holidays_in_year
        
        return output_source3_dict


    def __extract_incremental_data_from_source3(self, current_year):
        """
        extracting holidays data for current year for incremental ETL

        :param current_year
        :return: dictionary with all holidays of current year
        """     
        output_source3_dict = {}

        params = {
            "jahr": current_year,
            "nur_land": self.source3_params["BUNDESLAND"]
        }
        response = requests.get(self.source3_params["BASE_PATH"], params=params)
        holidays_in_year = response.json()
        output_source3_dict = holidays_in_year
        
        return output_source3_dict
    #endregion


    def initial_extraction(self):
        """
        get initial extractions for each data source

        :return: dictionary with a key for each source
        """ 
        output_sources_dict = {}


        #get output source1
        output_sources_dict["source1"] = self.__extract_initial_data_from_source1()


        #get start_date & current_date
        first_address_id = list(output_sources_dict["source1"]["data"].keys())[0]
        # extract date of the first available file in order to get the right data for sources 2 & 3
        first_month_file = list(output_sources_dict["source1"]["data"][first_address_id].keys())[0] #e.g. 2023-05.csv
        '''
        #for simulating stuff
        #first_month_file = "2019-07.csv"
        '''
        date_file_format = "%Y-%m"
        first_date = datetime.strptime(first_month_file.replace(".csv", ""), date_file_format).date()
        start_year = first_date.year


        #get output source2
        current_year = get_current_date().year
        output_sources_dict["source2"] = self.__extract_initial_data_from_source2(start_year=start_year, current_year=current_year)


        #get source3
        output_sources_dict["source3"] = output_source3_dict = self.__extract_initial_data_from_source3(start_year=start_year, current_year=current_year)

        return output_sources_dict


    def incremental_extraction(self):
        """
        get incremental extractions for each data source

        :return: dictionary with a key for each source
        """ 
        current_year = get_current_date().year
        output_sources_dict = {}
        output_sources_dict["source1"] = self.__extract_incremental_data_from_source1()
        output_sources_dict["source2"] = self.__extract_incremental_data_from_source2(current_year=current_year)
        output_sources_dict["source3"] = self.__extract_incremental_data_from_source3(current_year=current_year)
        return output_sources_dict
