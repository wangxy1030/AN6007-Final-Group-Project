#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 19 20:05:02 2025

@author: lkcynia
"""

import logging
import os
import pandas as pd
from datetime import datetime, timedelta
import json
from multiprocessing import Process

class ArchiveSystem:
    def __init__(self):
        self.yesterday = datetime.now() - timedelta(days=1)
        self.date_str = self.yesterday.strftime("%m-%d")
        self.year_str = self.yesterday.strftime("%Y")
        self.month_str = self.yesterday.strftime("%m")
        self.daily_file = None

        self.log_dir = "./logs"
        os.makedirs(self.log_dir, exist_ok=True)
        self._init_logger()

    def _init_logger(self):
        self.archiving_logger = logging.getLogger('archiving')
        self.archiving_logger.setLevel(logging.INFO)

        archiving_handler = logging.FileHandler(os.path.join(self.log_dir, "archiving.log"))
        archiving_handler.setLevel(logging.INFO)

        archiving_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", 
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        archiving_handler.setFormatter(archiving_formatter)

        self.archiving_logger.addHandler(archiving_handler)

    def log_archiving_info(self, message):
        self.archiving_logger.info(message)

    def log_archiving_error(self, message):
        self.archiving_logger.error(message, exc_info=True)

    def check_task_completion(self, task_name):
        try:
            with open(os.path.join(self.log_dir, "archiving.log"), "r") as log_file:
                logs = log_file.readlines()
                for log in logs:
                    if f"{task_name} completed successfully" in log:
                        return True
            return False
        except Exception as e:
            self.log_archiving_error(f"Error checking task completion for {task_name}: {e}")
            return False

    def archive_daily_reading(self):
        try:
            folder_path = f"{self.year_str}/{self.month_str}/daily reading"
            os.makedirs(folder_path, exist_ok=True)
            file_path = f"{folder_path}/{self.date_str}.csv"

            self.log_archiving_info(f"Starting archive_daily_reading for {self.date_str}")           
            # API
            # response = requests.get("") # api from vendor
            # if response.status_code != 200:
            #     log_error("Error fetching data from API")
            #     return
            
            # data = response.json()
            # df = pd.DataFrame(data)
            # df.to_csv(file_path, index=False)
            
            # 测试：从本地mock_data.json读数据
            with open("data_2.19.json", "r") as file:
                data = json.load(file)

            df = pd.DataFrame(data).T.reset_index()
            df.rename(columns={"index": "meter_id"}, inplace=True)
            df.to_csv(file_path, index=False)

            self.log_archiving_info(f"Daily readings archived successfully: {file_path}")
            self.daily_file = file_path
            return file_path

        except Exception as e:
            self.log_archiving_error(f"Failed to archive daily readings for {self.date_str}: {e}")
            return None

    def archive_monthly_reading(self):
        try:
            monthly_file = f"{self.year_str}/{self.month_str}/monthly_reading_{self.year_str}-{self.month_str}.csv"
            self.log_archiving_info(f"Starting archive_monthly_reading for {self.date_str}")
    
            if not os.path.exists(self.daily_file):
                self.log_archiving_error(f"Daily readings file not found: {self.daily_file}")
                return
    
            df_daily = pd.read_csv(self.daily_file)
            if "24:00" not in df_daily.columns:
                self.log_archiving_error("Column '24:00' not found in daily file.")
                return
    
            df_24h = df_daily[["meter_id", "24:00"]].copy()
            df_24h = df_24h.rename(columns={"24:00": self.date_str})
    
            if os.path.exists(monthly_file):
                df_monthly = pd.read_csv(monthly_file, dtype={"meter_id": str})
                if self.date_str in df_monthly.columns:
                    self.log_archiving_info(f"Date {self.date_str} already exists in {monthly_file}, updating values.")
                    df_monthly = df_monthly.drop(columns=[self.date_str])
    
                df_monthly = df_monthly.merge(df_24h, on="meter_id", how="outer")
            else:
                self.log_archiving_info(f"Creating new monthly file: {monthly_file}")
                df_monthly = df_24h.copy()
    
            df_monthly.to_csv(monthly_file, index=False, encoding="utf-8")
            self.log_archiving_info(f"Monthly reading file updated: {monthly_file}")
    
        except Exception as e:
            self.log_archiving_error(f"Failed to archive monthly readings: {e}")

    def archive_daily_consumption(self):
        try:
            folder_path = f"{self.year_str}/{self.month_str}/daily consumption"
            os.makedirs(folder_path, exist_ok=True)
            output_file = f"{folder_path}/{self.date_str}.csv"
    
            self.log_archiving_info(f"Starting archive_daily_consumption for {self.date_str}")
    
            if not os.path.exists(self.daily_file):
                self.log_archiving_error(f"Daily readings file not found: {self.daily_file}")
                return
    
            df = pd.read_csv(self.daily_file, dtype={"meter_id": str})
            time_slots = df.columns[1:]
            for col in time_slots:
                df[col] = df[col].astype(float)
    
            df_consumption = df.copy()
            df_consumption["00:00"] = 0.0
            for i in range(1, len(time_slots)):
                df_consumption[time_slots[i]] = df[time_slots[i]] - df[time_slots[i-1]]
                df_consumption[time_slots[i]] = df_consumption[time_slots[i]].round(2)
    
            df_consumption.to_csv(output_file, index=False)
    
            self.log_archiving_info(f"Daily consumption archived successfully: {output_file}")
    
        except Exception as e:
            self.log_archiving_error(f"Failed to archive daily consumption for {self.date_str}: {e}")

    def archive_monthly_consumption(self):
        try:
            folder_path = f"{self.year_str}/{self.month_str}"
            os.makedirs(folder_path, exist_ok=True)
            output_file = f"{folder_path}/monthly_consumption_{self.year_str}-{self.month_str}.csv"
    
            self.log_archiving_info(f"Starting archive_monthly_consumption for {self.date_str}")
    
            if not os.path.exists(self.daily_file):
                self.log_archiving_error(f"Daily readings file not found: {self.daily_file}")
                return
    
            df_daily = pd.read_csv(self.daily_file, dtype={"meter_id": str})
            if "24:00" not in df_daily.columns or "00:00" not in df_daily.columns:
                self.log_archiving_error("Columns '24:00' or '00:00' not found in daily file.")
                return
    
            df_daily["daily_consumption"] = df_daily["24:00"].astype(float) - df_daily["00:00"].astype(float)
            df_daily["daily_consumption"] = df_daily["daily_consumption"].round(2)
            
            df_daily = df_daily[["meter_id", "daily_consumption"]]
            df_daily = df_daily.rename(columns={"daily_consumption": self.date_str})  # 日期列 (MM-DD)
    
            if os.path.exists(output_file):
                df_monthly = pd.read_csv(output_file, dtype={"meter_id": str})
                if self.date_str in df_monthly.columns:
                    self.log_archiving_info(f"Date {self.date_str} already exists in {output_file}, updating values.")
                    df_monthly = df_monthly.drop(columns=[self.date_str])
    
                df_monthly = df_monthly.merge(df_daily, on="meter_id", how="outer")
            else:
                self.log_archiving_info(f"Creating new monthly consumption file: {output_file}")
                df_monthly = df_daily.copy()
    
            df_monthly = df_monthly.round(2)
            df_monthly.to_csv(output_file, index=False, encoding="utf-8")
            self.log_archiving_info(f"Monthly consumption file updated: {output_file}")
    
        except Exception as e:
            self.log_archiving_error(f"Failed to archive monthly consumption for {self.date_str}: {e}")

    def recover_from_crash(self):
        try:
            self.log_archiving_info("Starting recovery process...")

            if not self.check_task_completion("archive_daily_reading"):
                self.log_archiving_info("archive_daily_reading not completed. Restarting...")
                self.archive_daily_reading()
            else:
                self.log_archiving_info("archive_daily_reading already completed.")

            if self.daily_file:
                tasks = [
                    ("archive_monthly_reading", self.archive_monthly_reading),
                    ("archive_daily_consumption", self.archive_daily_consumption),
                    ("archive_monthly_consumption", self.archive_monthly_consumption),
                ]

                for task_name, task_func in tasks:
                    if not self.check_task_completion(task_name):
                        self.log_archiving_info(f"{task_name} not completed. Restarting...")
                        task_func()
                    else:
                        self.log_archiving_info(f"{task_name} already completed.")

            self.log_archiving_info("Recovery process completed.")

        except Exception as e:
            self.log_archiving_error(f"Error in recovery process: {e}")

    def run(self):
        self.recover_from_crash()

        self.archive_daily_reading()

        if self.daily_file:
            processes = []
            
            p1 = Process(target=self.archive_monthly_reading)
            processes.append(p1)
            p1.start()
            
            p2 = Process(target=self.archive_daily_consumption)
            processes.append(p2)
            p2.start()
            
            p3 = Process(target=self.archive_monthly_consumption)
            processes.append(p3)
            p3.start()
            
            for p in processes:
                p.join()

            self.log_archiving_info("All tasks completed successfully.")

if __name__ == "__main__":
    archive_system = ArchiveSystem()
    archive_system.run()