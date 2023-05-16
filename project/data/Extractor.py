import requests
import config #import config.py with secret tokens
import datetime
import pytz
import io
import pandas as pd
import json
from tqdm import tqdm, trange
import time


#helper method
def get_current_date()-> tuple[int, int, int]:
    # Get the current time in UTC
    current_time = datetime.datetime.now(pytz.utc)
    # Convert the current time to the timezone of Berlin
    berlin_timezone = pytz.timezone('Europe/Berlin')
    time = current_time.astimezone(berlin_timezone)
    #return time
    return time



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
            'API_TOKEN': config.github_api_token
        }
        #Source3: Holidays Data Soure (API-Feiertage)
        self.source3_params = {
            'BUNDESLAND': "nw",
            'BASE_URL': "https://get.api-feiertage.de",
        }

    #region source1: Bicycle Traffic Source (Github)


    def __get_source1_file_name_from_datetime(self, time: datetime.datetime) -> str:
        """
        generate csv-filename for input datetime

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
        get specific file (csv/json) from github repo

        :param path: str
        :return: string (csv/json)
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
    

    def __extract_iterative_data_from_source1(self):
        """
        extracting data from the data source 1 for iterative ETL

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




    def __extract_data_from_source2(self):
        # Code for extracting data from the data source
        pass




    def __extract_data_from_source3(self):
        # Code for extracting data from the data source
        pass


    def initial_extraction(self):
        #self.__extract_initial_data_from_source1()
        self.__extract_iterative_data_from_source1()


    def incremental_extraction(self):
        pass








path = "/main/site_min.json"
'''

counters = ["100031297","100031300", "100034978", "100034980", "100034981", "100034982", "100034983", "100035541", "100053305"]
path = counters[0]
time = get_current_date()
file_name = get_github_file_name_from_time(time=time)
path = f"main/100031297/{file_name}"
#contents = get_github_repo_contents(owner=OWNER, repo=REPO, path=path, token=GITHUB_TOKEN)


df = get_github_csv_file(owner=OWNER, repo=REPO, path=path, token=GITHUB_TOKEN)
df.head(2)

'''