from flask import Flask, jsonify, Response
import json
import os
import random
import datetime
import pandas as pd
import threading
import logging
from collections import OrderedDict

class Vendor:
    def __init__(self):
        self.app = Flask(__name__)
        self.data_file = "users.json"
        self.temp_csv = "temporary_meter.csv"
        self.log_dir = "./logs"
        os.makedirs(self.log_dir, exist_ok=True)
        self.users = self.load_users()
        self.setup_logging()
        self.setup_routes()
        self.generate_meter_data()
    
    def load_users(self):
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print("Json Error")
                return []
        return []

    def generate_meter_data(self):
        current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        hour = datetime.datetime.now().hour

        def generate_kWh():
            if 0 <= hour < 8:
                return round(random.uniform(0.35, 0.4), 2)
            elif 8 <= hour < 19:
                return round(random.uniform(0.2, 0.35), 2)
            else:
                return round(random.uniform(0.35, 0.45), 2)

        if os.path.exists(self.temp_csv):
            df = pd.read_csv(self.temp_csv, index_col=0)
        else:
            df = pd.DataFrame()

        last_values = df[df.columns[-1]].to_dict() if not df.empty else {}
        meter_data = {
            user["meter_id"]: round(last_values.get(user["meter_id"], 0) + generate_kWh(), 2)
            for user in self.users
        }
        
        new_data = pd.DataFrame(meter_data, index=[current_datetime]).T
        new_data.index.name = "Meter_ID"
        df = pd.concat([df, new_data], axis=1)
        df = df[~df.index.duplicated(keep="first")]
        df.to_csv(self.temp_csv)

        threading.Timer(18000, self.generate_meter_data).start()
    
    def setup_logging(self):
        self.api_logger = logging.getLogger('api')
        api_handler = logging.FileHandler(os.path.join(self.log_dir, "api_requests.log"))
        api_handler.setLevel(logging.INFO)
        api_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        api_handler.setFormatter(api_formatter)
        self.api_logger.addHandler(api_handler)
    
    def log_api_info(self, message):
        self.api_logger.info(message)
    
    def log_api_error(self, message):
        self.api_logger.error(message, exc_info=True)

    def read_csv(self, file_path):
        if not os.path.exists(file_path):
            return None
        df = pd.read_csv(file_path)
        return df.to_dict(orient="records") if not df.empty else None
    
    def remove_meter_id(self, data):
        if not data:
            return {}
        return {k: v for k, v in data[0].items() if k != "meter_id"}

    
    def setup_routes(self):
        @self.app.route('/vendor/previous_meter_data', methods=['GET'])
        def get_previous_meter_data():
            if not os.path.exists(self.temp_csv):
                return jsonify({"error": "No data available"}), 404

            df = pd.read_csv(self.temp_csv, index_col=0)
            df.columns = pd.to_datetime(df.columns, errors='coerce')
            today = datetime.datetime.now()
            yesterday = today - datetime.timedelta(days=1)

            start_time = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0) 
            end_time = datetime.datetime(today.year, today.month, today.day, 0, 0, 0) 
            df_filtered = df.loc[:, (df.columns >= start_time) & (df.columns <= end_time)]

            if df_filtered.empty:
                return jsonify({"error": "No previous day data available"}), 404
            result = df_filtered.to_dict(orient="index")
            formatted_result = {
                meter_id: {time.strftime("%Y/%m/%d %H:%M"): value for time, value in readings.items()}
                for meter_id, readings in result.items()
            }

            return jsonify(formatted_result)
        
        
        @self.app.route('/vendor/meter_data', methods=['GET'])
        def get_today_meter_data():
            if not os.path.exists(self.temp_csv):
                return jsonify({"error": "No data available"}), 404

            df = pd.read_csv(self.temp_csv, index_col=0)
            df.columns = pd.to_datetime(df.columns, errors='coerce')
            df = df.loc[:, pd.notna(df.columns)]
            df.index = df.index.astype(str)

            today = datetime.datetime.now().strftime("%Y-%m-%d")
            today_columns = [col for col in df.columns if col.strftime("%Y-%m-%d") == today]
            if not today_columns:
                return jsonify({"error": "No data available for today"}), 404

            meter_data = {
                str(meter_id): {col.strftime("%Y/%m/%d %H:%M"): df.loc[meter_id, col] for col in today_columns}
                for meter_id in df.index
            }
            return jsonify(meter_data)
        
        @self.app.route("/vendor/<meter_id>/daily_reading/<date>", methods=["GET"])
        def get_daily_reading(meter_id, date):
            year, month, day = date.split("-")
            file_path = f"{year}/{month}/daily reading/{month}-{day}.csv"
            data = self.read_csv(file_path)
            self.log_api_info(f"API request for daily reading: {meter_id} on {date}")
            filtered_data = [d for d in data if str(d["meter_id"]) == meter_id] if data else None
            return jsonify(self.remove_meter_id(filtered_data)) if filtered_data else jsonify({"error": "No data found"}), 404
        
        @self.app.route("/vendor/<meter_id>/daily_consumption/<date>", methods=["GET"])
        def get_daily_consumption(meter_id, date):
            year, month, day = date.split("-")
            file_path = f"{year}/{month}/daily consumption/{month}-{day}.csv"
            data = self.read_csv(file_path)
            self.log_api_info(f"API request for daily consumption: {meter_id} on {date}")
            filtered_data = [d for d in data if str(d["meter_id"]) == meter_id] if data else None
            return jsonify(self.remove_meter_id(filtered_data)) if filtered_data else jsonify({"error": "No data found"}), 404

        @self.app.route("/vendor/<meter_id>/monthly_reading/<year_month>", methods=["GET"])
        def get_monthly_reading(meter_id, year_month):
            year, month = year_month.split("-")
            file_path = f"{year}/{month}/monthly_reading_{year}-{month}.csv"
            data = self.read_csv(file_path)
            self.log_api_info(f"API request for monthly reading: {meter_id} in {year_month}")
            filtered_data = [d for d in data if str(d["meter_id"]) == meter_id] if data else None
            return jsonify(self.remove_meter_id(filtered_data)) if filtered_data else jsonify({"error": "No data found"}), 404
        
        @self.app.route("/vendor/<meter_id>/monthly_consumption/<year_month>", methods=["GET"])
        def get_monthly_consumption(meter_id, year_month):
            year, month = year_month.split("-")
            file_path = f"{year}/{month}/monthly_consumption_{year}-{month}.csv"
            data = self.read_csv(file_path)
            self.log_api_info(f"API request for monthly consumption: {meter_id} in {year_month}")
            filtered_data = [d for d in data if str(d["meter_id"]) == meter_id] if data else None
            return jsonify(self.remove_meter_id(filtered_data)) if filtered_data else jsonify({"error": "No data found"}), 404
    
    def run(self):
        self.app.run(use_reloader=False)
if __name__ == "__main__":
    vendor = Vendor()
    vendor.run()
