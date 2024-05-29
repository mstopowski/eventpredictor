import os
import glob
import json
import requests
import datetime

import pandas as pd
import multiprocess as mp


class BaseScraper:

    HEADERS = {
        "content-type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
    }

    @staticmethod
    def rangeofdates(start_date, end_date):
        for day in range(int((end_date - start_date).days) + 1):
            yield start_date + datetime.timedelta(day)
    
    @staticmethod
    def save_get_request_to_json(url, headers, file_name, save_path):
        response = requests.get(url=url, headers=headers)
        os.makedirs(save_path, exist_ok=True)
        with open(file_name, "w") as f:
            json.dump(response.json(), f)

    @staticmethod
    def csv_to_json(file_name, save_path, odds=False):
        df = pd.read_json(file_name)

        temp_list = []
        for i in range(df.shape[0]):
            row_data = df.iloc[i].values[0]
            if not odds:
                temp_list.append(pd.json_normalize(row_data, sep="_"))
            else:
                choices_df = pd.json_normalize(row_data['choices'])
                choices_df['id'] = df.index[i]
                temp_list.append(choices_df)

        df = pd.concat(temp_list, axis=0)

        if odds:
            df = df.pivot(index='id', columns='name', values='fractionalValue')
            df = df.reset_index()

        df.to_csv(save_path, index=False)

    @staticmethod
    def combine_csv_files(files_path, save_dir, file_name):
        temp_list = []
        for file in glob.iglob(files_path):
            temp_list.append(pd.read_csv(file))

        df = pd.concat(temp_list, axis=0)
        df = df.drop_duplicates(keep="last")
        df = df.reset_index(drop=True)

        df.to_csv(os.path.join(save_dir, file_name), index=False)


class FootballScraper(BaseScraper):

    EVENTS_URL = "https://api.sofascore.com/api/v1/sport/football/scheduled-events/"
    ODDS_URL = "https://api.sofascore.com/api/v1/sport/football/odds/1/"

    def __init__(self, start_date, end_date, raw_data_dir):
        self.start_date = start_date
        self.end_date = end_date
        self.raw_data_dir = raw_data_dir
        self.make_paths()

    def make_paths(self):
        self.scheduled_events = self.raw_data_dir / "scheduled_events"
        self.scheduled_events_json = self.scheduled_events / "json"
        self.scheduled_events_csv = self.scheduled_events / "csv"

        self.odds = self.raw_data_dir / "odds"
        self.odds_json = self.odds / "json"
        self.odds_csv = self.odds / "csv"

    def download_data_from_date(self, date, save_path, url):
        date_str = date.strftime("%Y-%m-%d")
        file_name = date_str + ".json"
        file_name = os.path.join(save_path, file_name)

        if os.path.exists(file_name):
            return None
 
        self.save_get_request_to_json(
            url=url + date_str,
            headers=FootballScraper.HEADERS,
            file_name=file_name,
            save_path=save_path
        )

    def download_events(self):
        for date in self.rangeofdates(self.start_date, self.end_date):
            self.download_data_from_date(
                date=date, 
                save_path=self.scheduled_events_json,
                url=FootballScraper.EVENTS_URL
            )

    def download_odds(self):
        for date in self.rangeofdates(self.start_date, self.end_date):
            self.download_data_from_date(
                date=date, 
                save_path=self.odds_json,
                url=FootballScraper.ODDS_URL
            )

    def json_to_csv(self, file, save_path, odds=False):
        file_name = file.split("/")[-1].split(".")[0] + ".csv"
        os.makedirs(save_path, exist_ok=True)
        save_path = os.path.join(save_path, file_name)

        if os.path.exists(save_path):
            return None

        self.csv_to_json(
            file_name=file,
            save_path=save_path,
            odds=odds
        )
    
    def multiprocess_files_convert(self, func, csv_dir, json_dir, odds=False):
        pool = mp.Pool()
        processes = [
            pool.apply_async(
                func,
                args=(
                    file,
                    csv_dir,
                    odds
                ),
            )
            for file in glob.iglob(os.path.join(json_dir, "*.json"))
        ]
        for p in processes:
            p.get()

    def convert_events(self):
        self.multiprocess_files_convert(
            func=self.json_to_csv,
            csv_dir=self.scheduled_events_csv,
            json_dir=self.scheduled_events_json,
        )

    def conert_odds(self):
        self.multiprocess_files_convert(
            func=self.json_to_csv,
            csv_dir=self.odds_csv,
            json_dir=self.odds_json,
            odds=True
        )

    def combine_events(self):
        self.combine_csv_files(
            files_path=os.path.join(self.scheduled_events_csv, "*.csv"),
            save_dir=self.scheduled_events,
            file_name="scheduled_events.csv",
        )

    def combine_odds(self):
        self.combine_csv_files(
            files_path=os.path.join(self.odds_csv, "*.csv"),
            save_dir=self.odds,
            file_name="odds.csv",
        )
