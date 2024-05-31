import os
import glob
import json
import requests
import datetime

import numpy as np
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
    def combine_csv_files(files_path, save_dir, file_name):
        temp_dir = save_dir / "temp"
        os.makedirs(temp_dir, exist_ok=True)

        temp_list = []
        for idx, file in enumerate(glob.iglob(files_path), 1):

            if idx % 5_000 == 0:
                df = pd.concat(temp_list, axis=0)
                df.to_csv(os.path.join(temp_dir, f"temp_{idx}.csv"), index=False)
                temp_list = []

            temp_list.append(pd.read_csv(file))

        if len(temp_list) > 0:
            df = pd.concat(temp_list, axis=0)
            df.to_csv(os.path.join(temp_dir, f"temp_{idx}.csv"), index=False)

        temp_list = []
        for file in glob.iglob(os.path.join(temp_dir, "*.csv")):
            temp_list.append(pd.read_csv(file))

        if len(temp_list) > 0:
            df = pd.concat(temp_list, axis=0)
            df = df.drop_duplicates(keep="last")
            df = df.reset_index(drop=True)

            df.to_csv(os.path.join(save_dir, file_name), index=False)

        for file in glob.iglob(os.path.join(temp_dir, "*.csv")):
            os.remove(file)


class FootballScraper(BaseScraper):

    EVENTS_URL = "https://api.sofascore.com/api/v1/sport/football/scheduled-events/"
    ODDS_URL = "https://api.sofascore.com/api/v1/sport/football/odds/1/"
    SINGLE_EVENT_URL = "https://api.sofascore.com/api/v1/event/"

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

        self.statistics = self.raw_data_dir / "statistics"
        self.statistics_json = self.statistics / "json"
        self.statistics_csv = self.statistics / "csv"

        self.incidents = self.raw_data_dir / "incidents"
        self.incidents_json = self.incidents / "json"
        self.incidents_csv = self.incidents / "csv"

        self.lineups = self.raw_data_dir / "lineups"
        self.lineups_json = self.lineups / "json"
        self.lineups_csv = self.lineups / "csv"

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
            save_path=save_path,
        )

    def json_to_csv(self, file, save_path, stat=None):
        file_name = file.split("/")[-1].split(".")[0] + ".csv"
        os.makedirs(save_path, exist_ok=True)
        save_path = os.path.join(save_path, file_name)

        if os.path.exists(save_path):
            return None

        self.csv_to_json(
            file_name=file,
            save_path=save_path,
            stat=stat,
        )

    def multiprocess_files_convert(self, func, csv_dir, json_dir, stat=None):
        pool = mp.Pool()
        processes = [
            pool.apply_async(
                func,
                args=(file, csv_dir, stat),
            )
            for file in glob.iglob(os.path.join(json_dir, "*.json"))
        ]
        for p in processes:
            p.get()

    def download_events(self):
        for date in self.rangeofdates(self.start_date, self.end_date):
            self.download_data_from_date(
                date=date, save_path=self.scheduled_events_json, url=FootballScraper.EVENTS_URL
            )

    def convert_events(self):
        self.multiprocess_files_convert(
            func=self.json_to_csv,
            csv_dir=self.scheduled_events_csv,
            json_dir=self.scheduled_events_json,
        )

    def combine_events(self):
        self.combine_csv_files(
            files_path=os.path.join(self.scheduled_events_csv, "*.csv"),
            save_dir=self.scheduled_events,
            file_name="scheduled_events.csv",
        )

    def download_odds(self):
        for date in self.rangeofdates(self.start_date, self.end_date):
            self.download_data_from_date(
                date=date, save_path=self.odds_json, url=FootballScraper.ODDS_URL
            )

    def conert_odds(self):
        self.multiprocess_files_convert(
            func=self.json_to_csv, csv_dir=self.odds_csv, json_dir=self.odds_json, stat="odds"
        )

    def combine_odds(self):
        self.combine_csv_files(
            files_path=os.path.join(self.odds_csv, "*.csv"),
            save_dir=self.odds,
            file_name="odds.csv",
        )

    def download_statistics(self):
        self.download_stats(name_of_stat="statistics", json_dir=self.statistics_json)

    def convert_statistics(self):
        self.multiprocess_files_convert(
            func=self.json_to_csv,
            csv_dir=self.statistics_csv,
            json_dir=self.statistics_json,
            stat="statistics",
        )

    def combine_statistics(self):
        self.combine_csv_files(
            files_path=os.path.join(self.statistics_csv, "*.csv"),
            save_dir=self.statistics,
            file_name="statistics.csv",
        )

    def download_incidents(self):
        self.download_stats(name_of_stat="incidents", json_dir=self.incidents_json)

    def convert_incidents(self):
        self.multiprocess_files_convert(
            func=self.json_to_csv,
            csv_dir=self.incidents_csv,
            json_dir=self.incidents_json,
            stat="incidents",
        )

    def combine_incidents(self):
        self.combine_csv_files(
            files_path=os.path.join(self.incidents_csv, "*.csv"),
            save_dir=self.incidents,
            file_name="incidents.csv",
        )

    def download_lineups(self):
        self.download_stats(name_of_stat="lineups", json_dir=self.lineups_json)

    def convert_lineups(self):
        self.multiprocess_files_convert(
            func=self.json_to_csv,
            csv_dir=self.lineups_csv,
            json_dir=self.lineups_json,
            stat="lineups",
        )

    def combine_lineups(self):
        self.combine_csv_files(
            files_path=os.path.join(self.lineups_csv, "*.csv"),
            save_dir=self.lineups,
            file_name="lineups.csv",
        )

    def download_stats(self, name_of_stat, json_dir):
        downloads_df = pd.read_csv(os.path.join(self.raw_data_dir, "to_download.csv"))
        statistics_ids = downloads_df.loc[
            (downloads_df[name_of_stat] == 0) & (downloads_df[f"{name_of_stat}_status_code"] == 0),
            "id",
        ].values

        for idx, id in enumerate(statistics_ids):
            file_name = str(id) + ".json"
            save_path = os.path.join(json_dir, file_name)
            if os.path.exists(save_path):
                continue

            url = self.SINGLE_EVENT_URL + str(id) + f"/{name_of_stat}"
            response = requests.get(url, headers=FootballScraper.HEADERS)
            data = response.json()
            status_code = response.status_code

            print(idx, id, status_code)

            if "error" not in data:
                os.makedirs(json_dir, exist_ok=True)
                with open(save_path, "w") as f:
                    json.dump(data, f)
                downloads_df.loc[
                    downloads_df["id"] == id, [name_of_stat, f"{name_of_stat}_status_code"]
                ] = (int(status_code == 200), status_code)

            else:
                downloads_df.loc[
                    downloads_df["id"] == id,
                    [f"{name_of_stat}_error", f"{name_of_stat}_status_code"],
                ] = (1, status_code)

        downloads_df.to_csv(os.path.join(self.raw_data_dir, "to_download.csv"), index=False)

    def generate_download_file(
        self, countries_to_remove=None, status_to_keep=None, min_no_events=2000
    ):
        if os.path.exists(os.path.join(self.raw_data_dir, "to_download.csv")):
            print("File exists already.")
            return None

        cols_to_select = [
            "id",
            "tournament_slug",
            "tournament_category_slug",
            "status_description",
            "status_type",
        ]
        events_df = pd.read_csv(
            os.path.join(self.scheduled_events, "scheduled_events.csv"), usecols=cols_to_select
        )
        events_df = events_df.drop_duplicates()
        if countries_to_remove is not None:
            events_df = events_df[~events_df["tournament_category_slug"].isin(countries_to_remove)]

        if min_no_events is not None:
            selected_tournaments = (
                events_df[["tournament_category_slug", "tournament_slug", "id"]]
                .groupby(["tournament_category_slug", "tournament_slug"], as_index=False)["id"]
                .count()
            )
            selected_tournaments = (
                selected_tournaments[selected_tournaments["id"] >= min_no_events]
                .reset_index(drop=True)
                .rename(columns={"id": "count"})
            )
            selected_tournaments_set = (
                selected_tournaments["tournament_category_slug"]
                + selected_tournaments["tournament_slug"]
            ).values
            events_df = events_df.loc[
                (events_df["tournament_category_slug"] + events_df["tournament_slug"]).isin(
                    selected_tournaments_set
                )
            ]

        if min_no_events is not None:
            events_df = events_df[events_df["status_description"].isin(status_to_keep)]

        events_df = events_df[["id"]].sort_values("id").reset_index(drop=True)
        events_df[["statistics", "statistics_status_code", "statistics_error"]] = 0
        events_df[["incidents", "incidents_status_code", "incidents_error"]] = 0
        events_df[["lineups", "lineups_status_code", "lineups_error"]] = 0

        events_df.to_csv(os.path.join(self.raw_data_dir, "to_download.csv"), index=False)

    @staticmethod
    def csv_to_json(file_name, save_path, stat=None):
        df = pd.read_json(file_name)

        temp_list = []
        for i in range(df.shape[0]):
            row_data = df.iloc[i].values[0]

            if stat == "statistics":
                group_temp_list = []
                for group_data in row_data.get("groups"):
                    stats_df = pd.DataFrame(group_data.get("statisticsItems"))
                    stats_df["groupName"] = group_data.get("groupName")
                    group_temp_list.append(stats_df)

                group_df = pd.concat(group_temp_list, axis=0)
                group_df["period"] = row_data.get("period")
                temp_list.append(group_df)

            if stat == "odds":
                choices_df = pd.json_normalize(row_data["choices"])
                choices_df["id"] = df.index[i]
                temp_list.append(choices_df)

            if stat == "incidents":
                temp_list.append(pd.json_normalize(row_data, sep="_"))

            if stat == None:
                temp_list.append(pd.json_normalize(row_data, sep="_"))

        if stat == "lineups":
            temp_df = pd.json_normalize(df.iloc[0]["home"], sep="_")
            temp_df["side"] = "home"
            temp_list.append(temp_df)

            temp_df = pd.json_normalize(df.iloc[0]["away"], sep="_")
            temp_df["side"] = "away"
            temp_list.append(temp_df)

        if len(temp_list) == 0:
            return None

        df = pd.concat(temp_list, axis=0)

        if stat == "odds":
            df = df.pivot(index="id", columns="name", values="fractionalValue")
            df = df.reset_index()

        if stat == "statistics":
            cols_to_select = [
                "name",
                "home",
                "away",
                "homeValue",
                "awayValue",
                "homeTotal",
                "awayTotal",
                "period",
            ]
            df = df.loc[:, [col for col in df.columns if col in cols_to_select]]

            temp_list = []
            for period in df["period"].unique():
                name_temp_list = []

                period_df = df.loc[df["period"] == period].drop(columns=["period"])
                for name in period_df["name"].unique():

                    name_df = (
                        period_df.loc[period_df["name"] == name]
                        .drop(columns=["name"])
                        .reset_index(drop=True)
                    )
                    name_df.columns = [
                        col + f"_{name.replace(' ', '_').lower()}" for col in name_df.columns
                    ]
                    name_temp_list.append(name_df)

                name_temp_df = (
                    pd.concat(name_temp_list, axis=1).dropna(axis=1).reset_index(drop=True)
                )
                name_temp_df.columns = [col + f"_{period.lower()}" for col in name_temp_df.columns]
                temp_list.append(name_temp_df)

            df = pd.concat(temp_list, axis=1).dropna(axis=1)
            df = pd.concat([pd.DataFrame([id], columns=["id"]), df], axis=1)

        if stat == "incidents":
            id = file_name.split("/")[-1].split(".")[0]
            cols_to_select = [
                "text",
                "homeScore",
                "awayScore",
                "time",
                "incidentType",
                "isHome",
                "incidentClass",
            ]
            df_columns = [col for col in df.columns if col in cols_to_select]
            df = df[df_columns]
            df = df.reset_index(drop=True).sort_index(ascending=False)
            if "period" in df["incidentType"].values:
                if "HT" in df["text"].values:
                    df = FootballScraper.with_ht(df, id, period=True, ht=True)
                else:
                    df = FootballScraper.with_ht(df, id, period=True, ht=False)
            else:
                df = FootballScraper.with_ht(df, id, period=False, ht=False)

        if stat == "lineups":
            if "statistics_rating" not in df.columns:
                return None
            cols_to_select = [
                "id",
                "side",
                "position",
                "substitute",
                "player_slug",
                "statistics_rating",
            ]
            df = df.loc[:, [col for col in cols_to_select if col in df.columns]]

            df = df.dropna()

            home_team = df.query('side == "home"')
            home_team_avg = home_team["statistics_rating"].mean()
            home_team = home_team.groupby("position", as_index=False)["statistics_rating"].mean()

            away_team = df.query('side == "away"')
            away_team_avg = away_team["statistics_rating"].mean()
            away_team = away_team.groupby("position", as_index=False)["statistics_rating"].mean()

            id = file_name.split("/")[-1].split(".")[0]
            array_to_store = [id]
            for side_df in [home_team, away_team]:
                for pos in ["G", "D", "M", "F"]:
                    if pos in side_df["position"].unique():
                        array_to_store.append(
                            side_df.loc[side_df["position"].eq(pos), "statistics_rating"].values[0]
                        )
                    else:
                        array_to_store.append(np.nan)
            array_to_store.extend([home_team_avg, away_team_avg])

            df = pd.DataFrame(
                [array_to_store],
                columns=[
                    "id",
                    "Gsc_H",
                    "Dsc_H",
                    "Msc_H",
                    "Fsc_H",
                    "Gsc_A",
                    "Dsc_A",
                    "Msc_A",
                    "Fsc_A",
                    "sc_H",
                    "sc_A",
                ],
            )

        df.to_csv(save_path, index=False)

    @staticmethod
    def with_ht(df, id, period=False, ht=False):
        home_stats = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        away_stats = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        half_time = 0
        score = [0, 0]

        if period & ht:
            for row in df.iterrows():
                row_data = row[1]

                incidentType = row_data["incidentType"]

                if "isHome" in df.columns:
                    isHome = row_data["isHome"]
                    if isHome == True:
                        stats = home_stats
                    elif isHome == False:
                        stats = away_stats

                    incidentClass = row_data["incidentClass"]

                    if (incidentType == "card") & (incidentClass == "yellow"):
                        stats[0 + half_time] += 1
                    elif (incidentType == "card") & (incidentClass == "red"):
                        stats[2 + half_time] += 1
                    elif (incidentType == "card") & (incidentClass == "yellowRed"):
                        stats[4 + half_time] += 1
                    elif incidentType == "goal":
                        if (row_data["homeScore"] - score[0]) + (
                            row_data["awayScore"] - score[1]
                        ) > 0:
                            if (row_data["homeScore"] - score[0]) > (
                                row_data["awayScore"] - score[1]
                            ):
                                stats = home_stats
                            else:
                                stats = away_stats
                            stats[6 + half_time] += 1
                            score[0] = row_data["homeScore"]
                            score[1] = row_data["awayScore"]
                            if incidentClass == "penalty":
                                stats[10] += 1

                        if incidentClass == "ownGoal":
                            stats[12] += 1
                    elif incidentType == "inGamePenalty":
                        stats[11] += 1
                    # elif (incidentType == 'goal') & (incidentClass == 'ownGoal'):
                    #     stats[12] += 1
                    elif incidentType == "substitution":
                        stats[13 + half_time] += 1

                if incidentType == "period":
                    home_stats[8 + half_time] = int(row_data["homeScore"])
                    away_stats[8 + half_time] = int(row_data["awayScore"])
                    text = row_data["text"]
                    if text == "HT":
                        half_time = 1
        else:
            for row in df.iterrows():
                row_data = row[1]
                time = row_data["time"]
                if time > 45:
                    half_time = 1
                incidentType = row_data["incidentType"]

                if "isHome" in df.columns:
                    isHome = row_data["isHome"]
                    if isHome == True:
                        stats = home_stats
                    elif isHome == False:
                        stats = away_stats

                    incidentClass = row_data["incidentClass"]

                    if (incidentType == "card") & (incidentClass == "yellow"):
                        stats[0 + half_time] += 1
                    elif (incidentType == "card") & (incidentClass == "red"):
                        stats[2 + half_time] += 1
                    elif (incidentType == "card") & (incidentClass == "yellowRed"):
                        stats[4 + half_time] += 1
                    elif incidentType == "goal":
                        if (row_data["homeScore"] - score[0]) + (
                            row_data["awayScore"] - score[1]
                        ) > 0:
                            if (row_data["homeScore"] - score[0]) > (
                                row_data["awayScore"] - score[1]
                            ):
                                stats = home_stats
                            else:
                                stats = away_stats
                            stats[6 + half_time] += 1
                            score[0] = row_data["homeScore"]
                            score[1] = row_data["awayScore"]
                            if incidentClass == "penalty":
                                stats[10] += 1
                        if incidentClass == "ownGoal":
                            stats[12] += 1
                    elif incidentType == "inGamePenalty":
                        stats[11] += 1
                    # elif (incidentType == 'goal') & (incidentClass == 'ownGoal'):
                    #     stats[12] += 1
                    elif incidentType == "substitution":
                        stats[13 + half_time] += 1
                if incidentType == "period":
                    home_stats[8 + half_time] = int(row_data["homeScore"])
                    away_stats[8 + half_time] = int(row_data["awayScore"])

        df = pd.DataFrame(
            data=[home_stats + away_stats],
            columns=[
                "hy_1",
                "hy_2",
                "hr_1",
                "hr_2",
                "hyr_1",
                "hyr_2",
                "hg_1",
                "hg_2",
                "h_ht",
                "h_ft",
                "hp",
                "hpm",
                "hog",
                "hs_1",
                "hs_2",
                "ay_1",
                "ay_2",
                "ar_1",
                "ar_2",
                "ayr_1",
                "ayr_2",
                "ag_1",
                "ag_2",
                "a_ht",
                "a_ft",
                "ap",
                "apm",
                "aog",
                "as_1",
                "as_2",
            ],
        )
        df = df.replace({0: np.nan})
        df["id"] = id

        return df
