import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))
from extraction.boxoffice_api.validator import Validator
from bs4 import BeautifulSoup
import requests
import pandas as pd
import concurrent.futures
import calendar


class BoxOffice:
    """
    A package For getting Box office Information
    """

    def __init__(self, api_key=None, outputformat: str = "dict"):
        """
        initial class
        :param api_key: an api key from https://www.omdbapi.com/ if you provide an api key, you will also get
        movie poster url and movie description director and artists
        a free type of api provides 1000 calls per day so use it carefully
        """
        self._api_key = api_key
        self._output = []
        self._output_format = outputformat
        self._validate = Validator()

    @staticmethod
    def check_results(url):
        try:
            response = requests.get(url).text
            daily_soap = BeautifulSoup(response, "html.parser")
            table = daily_soap.find("table", "mojo-body-table")
            if table:
                table_rows = table.find_all("tr")
                return table_rows
            else:
                print("We couldn't find any result for this")
                return None
        except Exception as e:
            print(f"Error occurred: {e} Check Your Connection or wait  afew Second")

    def get_weekly(self, year: int, week: int):
        """
        get weekly information about box office
        :param year: year you want to get the information from must be a positive integer between 1982, and current year
        :param week: week you want to get information from must be a positive integer number between 1 and 52
        :return: a list of dictionary's contains movie information
        """
        validator = Validator()
        if validator.check_weekly(year=year, week=week):
            weekly_url = f"https://www.boxofficemojo.com/weekly/{year}W{week}/?ref_=bo_wly_table_1"
            if self.check_results(url=weekly_url):
                soap = self.check_results(url=weekly_url)
                result = self._collect_data(soap=soap)
                if self._output_format == "DF":
                    df = pd.DataFrame(result)
                    return df
                return result


    def _collect_data(self, soap):
        self._output = []
        headers = []
        requests_cache = {}  # Cache API requests to avoid duplicates

        for index, row in enumerate(soap):
            if index == 0:
                th_element = row.find_all("th")
                headers = [th.get_text().replace("\n", "") for th in th_element if "hidden" not in str(th)]
            else:
                td_element = row.find_all("td")
                local_list = [td.get_text().replace("\n", "") for td in td_element if "hidden" not in str(td)]
                if len(local_list) < 3:
                    continue  # Skip rows with insufficient data
                title = local_list[2]
                local_dict = {header: local_list[i] for i, header in enumerate(headers)}

                if title not in requests_cache:
                    if self._api_key is not None:
                        api_url = f"https://www.omdbapi.com/?t={title}&apikey={self._api_key}"
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            api_response = executor.submit(requests.get, api_url).result().json()
                        requests_cache[title] = api_response
                    else:
                        requests_cache[title] = {}

                for key, val in requests_cache[title].items():
                    local_dict[key] = val

                self._output.append(local_dict)

        return self._output