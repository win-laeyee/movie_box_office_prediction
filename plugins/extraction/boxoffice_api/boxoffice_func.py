import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))
from extraction.boxoffice_api.boxoffice_app import BoxOffice 
from googlecloud.upload_initial_data_gcs import delete_many_blobs, upload_many_blobs_with_transfer_manager
import os
import logging
from importlib import reload
import numpy as np
import pandas as pd
from datetime import datetime


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
                for week in weeks_array[50:53]:
                    df = pd.concat([df, data_by_year_week(box_office_obj, year, week)], axis=0)
            else:
                #at least 2 weeks worth of data every run (to ensure no missing data in dag)
                for week in weeks_array[max(0, now_week-4):(now_week-2)]:
                    df = pd.concat([df, data_by_year_week(box_office_obj, year, week)], axis=0)
                    
        logging.info(f"End Data Extraction")
    
        #upload copy to gcs
        script_dir = os.path.dirname(os.path.realpath(__file__))
        plugins_dir = os.path.dirname(os.path.dirname(script_dir))
        interested_dir = os.path.join(plugins_dir, "/googlecloud")
        json_path = os.path.join(interested_dir, "is3107-418809-62c002a9f1f7.json")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

        file_path = os.path.join(os.path.dirname(plugins_dir), "historical_data") 
        str_directory = os.path.join(file_path, 'update_data/boxofficemojo')
    
        folder_path = file_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        date_now = datetime.now().date
        df.to_csv(os.path.join(folder_path, f"boxofficemojo_data_{start_year}{end_year}.csv"), index=False)

        try:   
            bucket_name = "movies_tmdb"
            directory = Path(str_directory)
            filenames = [f"boxofficemojo_data_{start_year}{end_year}.csv"]
            delete_many_blobs(bucket_name, filenames)
            upload_many_blobs_with_transfer_manager(bucket_name, filenames=filenames, source_directory=str_directory)
            #remove directory after upload
            if os.path.exists(folder_path):
                os.rmdir(folder_path)
        except Exception as e:
            print(f"Error in uploading weekly domestic performance data to cloud storage \n Error details: {e}")

        return df
    else:
        raise ValueError("Start Year or End Year provided not Valid")

if __name__ == "__main__":
    #get_batch_dataset(start_year=2021, end_year=2023)
    #get_batch_dataset(start_year=2024, end_year=2024)
    pass