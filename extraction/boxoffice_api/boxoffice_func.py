import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))
from extraction.boxoffice_api import BoxOffice 
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

def get_batch_dataset(start_year:int=2021, end_year:int=2024) -> None:
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

        for year in np.array(range(start_year, now_year+1)):
            logging.info(f"{year=}")
            if year < now_year:
                for week in weeks_array:
                    df = pd.concat([df, data_by_year_week(box_office_obj, year, week)], axis=0)
            else:
                for week in weeks_array[0:now_week-2]:
                    df = pd.concat([df, data_by_year_week(box_office_obj, year, week)], axis=0)
                    
        logging.info(f"End Data Extraction")
        
        data_path = "./raw_data/boxofficemojo_rawdata"
        if not os.path.exists(data_path):
            os.makedirs(data_path)

        df.to_csv(os.path.join(data_path, f"boxofficemojo_data_{start_year}{end_year}.csv"), index=False)

    else:
        raise ValueError("Start Year or End Year provided not Valid")

    return None

if __name__ == "__main__":
    #get_batch_dataset(start_year=2021, end_year=2023)
    #get_batch_dataset(start_year=2024, end_year=2024)
    pass