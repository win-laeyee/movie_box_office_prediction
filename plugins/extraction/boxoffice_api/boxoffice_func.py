import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))
from extraction.boxoffice_api.boxoffice_app import BoxOffice 
from googlecloud.upload_initial_data_gcs import delete_many_blobs, upload_many_blobs_with_transfer_manager, upload_blob
import os
import logging
from importlib import reload
import numpy as np
import pandas as pd
from datetime import datetime
import shutil


def weeks_str() -> np.array:
    return np.array(["0"+str(i) for i in range(1,10)] + [str(i) for i in range(10,53)])


def data_by_year_week(box_office_obj, year:int, week:str) -> pd.DataFrame:
    sub_df = box_office_obj.get_weekly(year=year, week=week)
    sub_df.insert(0, "week", int(week))
    sub_df.insert(0, "year", year)
    return sub_df

def get_batch_dataset(datapath:str, start_year:int=2021, end_year:int=2024) -> None:
    #configuration
    reload(logging)
    logging.basicConfig(level=logging.INFO)

    # note that 1 year has 52 weeks
    box_office_obj = BoxOffice(outputformat="DF")
    df = pd.DataFrame()
    now_year = datetime.now().year
    now_week = datetime.now().isocalendar()[1] #get until now week - 2: ensure there's data)
    weeks_array = weeks_str()
    
    if start_year <= now_year and end_year <= now_year:
        logging.info(f"Start Data Extraction")

        for year in np.array(range(start_year, end_year+1)):
            logging.info(f"{year=}")
            if year < now_year:
                for week in weeks_array:
                    df = pd.concat([df, data_by_year_week(box_office_obj, year, week)], axis=0)
            else:
                for week in weeks_array[0:now_week-2]:
                    df = pd.concat([df, data_by_year_week(box_office_obj, year, week)], axis=0)
                    
        logging.info(f"End Data Extraction")
        
        data_path = datapath
        if not os.path.exists(data_path):
            os.makedirs(data_path)

        df.to_csv(os.path.join(data_path, f"boxofficemojo_data_{start_year}{end_year}.csv"), index=False)

    else:
        raise ValueError("Start Year or End Year provided not Valid")

    return None

#### Updating
def get_update_batch_dataset(year=int) -> pd.DataFrame:
    #configuration
    reload(logging)
    logging.basicConfig(level=logging.INFO)

    # note that 1 year has 52 weeks
    start_year = year
    end_year = year
    box_office_obj = BoxOffice(outputformat="DF")
    df = pd.DataFrame()
    now_year = datetime.now().year
    now_week = datetime.now().isocalendar()[1] #get until now week - 2: ensure there's data)
    weeks_array = weeks_str()
    
    if start_year <= now_year and end_year <= now_year:
        logging.info(f"Start Data Extraction")

        for year in np.array(range(start_year, end_year+1)):
            logging.info(f"{year=}")
            if year < now_year:
                for week in weeks_array[48:53]:
                    df = pd.concat([df, data_by_year_week(box_office_obj, year, week)], axis=0)
            else:
                #at least 4 weeks worth of data every run (to ensure no missing data in dag)
                for week in weeks_array[max(0, now_week-6):(now_week-2)]:
                    df = pd.concat([df, data_by_year_week(box_office_obj, year, week)], axis=0)
                    
        logging.info(f"End Data Extraction")
    
        #upload copy to gcs
        script_dir = os.path.dirname(os.path.realpath(__file__))
        plugins_dir = os.path.dirname(os.path.dirname(script_dir))
        interested_dir = os.path.join(plugins_dir, "/googlecloud")
        json_path = os.path.join(interested_dir, "is3107-418809-92db84ea97f6.json")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

        file_path = os.path.join(os.path.dirname(plugins_dir), "historical_data") 
        str_directory = os.path.join(file_path, 'update_data/boxofficemojo')
    
        folder_path = str_directory
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        date_now = datetime.now().date()
        df.to_csv(os.path.join(folder_path, f"update_boxofficemojo_{date_now}.csv"), index=False)

        try:   
            bucket_name = "update_movies_tmdb"
            filename = f"update_boxofficemojo_{date_now}.csv"
            upload_blob(bucket_name, source_file_name = os.path.join(folder_path,filename), destination_blob_name=filename)
            #remove local directory after upload
            if os.path.exists(folder_path):
                shutil.rmtree(folder_path)
        except Exception as e:
            print(f"Error in uploading weekly domestic performance data to cloud storage \n Error details: {e}")

        return df
    else:
        raise ValueError("Start Year or End Year provided not Valid")
    

def get_update_batch_dataset_by_week(week=int, year=int) -> pd.DataFrame:
    #configuration
    reload(logging)
    logging.basicConfig(level=logging.INFO)

    # note that 1 year has 52 weeks
    start_year = year
    end_year = year
    box_office_obj = BoxOffice(outputformat="DF")
    df = pd.DataFrame()
    now_year = datetime.now().year
    now_week = datetime.now().isocalendar()[1] #get until now week - 2: ensure there's data)

    weeks_array = np.concatenate((weeks_str(), weeks_str()))
    year_array = np.concatenate((np.repeat(year-1, 52), np.repeat(year, 52)))
    week_end_index = week + 52 - 1 - 2
    week_start_index = week + 52 - 6
    
    if start_year <= now_year and end_year <= now_year:
        logging.info(f"Start Data Extraction")

        for i in range(week_start_index, week_end_index+1):
            logging.info(f"{year_array[i]=}; {weeks_array[i]=}")
            df = pd.concat([df, data_by_year_week(box_office_obj, year_array[i], weeks_array[i])], axis=0)
                    
        logging.info(f"End Data Extraction")
    
        #upload copy to gcs
        script_dir = os.path.dirname(os.path.realpath(__file__))
        plugins_dir = os.path.dirname(os.path.dirname(script_dir))
        interested_dir = os.path.join(plugins_dir, "/googlecloud")
        json_path = os.path.join(interested_dir, "is3107-418809-92db84ea97f6.json")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

        file_path = os.path.join(os.path.dirname(plugins_dir), "historical_data") 
        str_directory = os.path.join(file_path, 'update_data/boxofficemojo')
    
        folder_path = str_directory
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        date_now = datetime.now().date()
        df.to_csv(os.path.join(folder_path, f"update_boxofficemojo_{week}_{year}.csv"), index=False)

        try:   
            bucket_name = "update_movies_tmdb"
            filename = f"update_boxofficemojo_{week}_{year}.csv"
            upload_blob(bucket_name, source_file_name = os.path.join(folder_path,filename), destination_blob_name=filename)
            #remove local directory after upload
            if os.path.exists(folder_path):
                shutil.rmtree(folder_path)
        except Exception as e:
            print(f"Error in uploading weekly domestic performance data to cloud storage \n Error details: {e}")

        return df
    else:
        raise ValueError("Start Year or End Year provided not Valid")

if __name__ == "__main__":
    #get_batch_dataset(start_year=2021, end_year=2023)
    #get_batch_dataset(start_year=2024, end_year=2024)
    pass