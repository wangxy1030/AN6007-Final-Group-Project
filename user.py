import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import io
import base64
from flask import Flask, request, redirect, url_for, send_file, render_template_string
import json
import logging
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import threading
from vendor import Vendor

app = Flask(__name__)
users = []

LOG_FILE = "reg.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(message)s')

def log_action(action, details):
    log_entry = f"{action}: {details}"
    logging.info(log_entry)

    with open(LOG_FILE, "a") as log_file:
        log_file.write(log_entry + "\n")

DATA_FILE = "users.json"
def load_users():
    global users
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            try:
                users = json.load(f)
            except json.JSONDecodeError:
                users = []

def save_users():
    with open(DATA_FILE, "w") as f:
        json.dump(users, f, indent=4)

# Initialize user data at startup
load_users()

# Home page (Login with Meter ID)
@app.route('/', methods=['GET', 'POST'])
def home():
    error_message = None
    if request.method == 'POST':
        meter_id = request.form.get('meter_id', '').strip()
        password = request.form.get('password', '')
        user = next((u for u in users if u['meter_id'] == meter_id), None)
        if not user:
            error_message = "Meter ID not found. Please register."
            log_action("Login Failed", f"Meter ID {meter_id} not found")
        elif user['password'] != password:
            error_message = "Incorrect password."
            log_action("Login Failed", f"Incorrect password for Meter ID {meter_id}")
        else:
            log_action("Login Success", f"Meter ID: {meter_id}")
            return redirect(url_for('query', meter_id=meter_id))
    return render_template_string('''
<html>
  <div id="rightdiv">
     <p>Please enter your Meter ID and password then click submit</p>
     
     <form action="/" method="post">
          <label for="meter_id">
            <strong>Meter ID</strong>
          </label>
          <input type="text" id="meter_id" placeholder="XXX-XXX-XXX" name="meter_id" pattern="\\d{3}-\\d{3}-\\d{3}" required>
          
          <label for="password">
            <strong>Password</strong>
          </label>
          <input type="password" id="password" placeholder="Enter your password" name="password" required>
          
          <input type="submit" value="Login">
     </form>

     {% if error_message %}
        <p style="color: red;">{{ error_message }}</p>
     {% endif %}

     <p>Don't have an account? <a href="/reg">Sign up</a></p>
  </div>
</html>
''', error_message=error_message)

# Registration page
@app.route('/reg', methods=['GET', 'POST'])
def reg():
    error_message = None
    success_message = None
    if request.method == 'POST':
        meter_id = request.form.get('meter_id', '').strip()
        region = request.form['region']
        dwelling_type = request.form['dwelling_type']
        password = request.form['password']
        confirm_password = request.form['confirm_password']

        if password != confirm_password:
            error_message = "Passwords do not match."
        elif not meter_id or not meter_id.replace('-', '').isdigit() or len(meter_id.replace('-', '')) != 9:
            error_message = "Invalid Meter ID format. Please use XXX-XXX-XXX format."
        elif any(u['meter_id'] == meter_id for u in users):
            error_message = "Meter ID already registered."
        else:
            user_data = {"meter_id": meter_id, "region": region, "dwelling_type": dwelling_type, "password": password}
            users.append(user_data)
            log_action("New Registration", f"Meter ID: {meter_id}, Region: {region}, Dwelling Type: {dwelling_type}")
            save_users()  # Save to file after registration
            success_message = "Registration successful! Please click below to login."
    
    return render_template_string('''
<html>
  <div id="signupdiv">
     <p>Please create a new user account</p>
     <form action="/reg" method="post">
          <label for="meter_id">
            <strong>Meter ID</strong>
          </label>
          <input type="text" id="meter_id" placeholder="XXX-XXX-XXX" name="meter_id" pattern="\\d{3}-\\d{3}-\\d{3}" required>  
          <label for="region">
            <strong>Region</strong>
          </label>
          <select name="region" id="region" required>
              <option value="Central Region">Central Region</option>
              <option value="North Region">North Region</option>
              <option value="West Region">West Region</option>
              <option value="North East Region">North East Region</option>
              <option value="East Region">East Region</option>
          </select>     
          <label for="dwelling_type">
            <strong>Dwelling Type</strong>
          </label>
          <select name="dwelling_type" id="dwelling_type" required>
              <option value="1-room / 2-room">1-room / 2-room</option>
              <option value="3-room">3-room</option>
              <option value="4-room">4-room</option>
              <option value="5-room and Executive">5-room and Executive</option>
              <option value="Private Apartments and Condominiums">Private Apartments and Condominiums</option>
              <option value="Landed Properties">Landed Properties</option>
          </select>    
          <label for="password">
            <strong>Password</strong>
          </label>
          <input type="password" id="password" placeholder="Create a password" name="password" required>
          <label for="confirm_password">
            <strong>Confirm Password</strong>
          </label>
          <input type="password" id="confirm_password" placeholder="Confirm password" name="confirm_password" required>
          <input type="submit" value="Sign Up">
     </form>
     
     {% if error_message %}
        <p style="color: red;">{{ error_message }}</p>
     {% endif %}
     
     {% if success_message %}
        <p style="color: green;">{{ success_message }}</p>
        <p><a href="/">Click here to login</a></p>
     {% endif %}
  </div>
</html>
''', error_message=error_message, success_message=success_message)

@app.route('/view_users')
def view_users():
    filename = 'users.json'
    with open(filename, 'w') as f:
        json.dump(users, f, indent=4)
    return send_file(filename, as_attachment=True)
    
@app.route('/query/<meter_id>', methods=['GET'])
def query(meter_id):
    return f"""
<html>
  <div id="signupdiv">
     <p>Please select the time period for your electricity consumption query</p>
          <label for="period">
            <strong>Query period</strong>
          </label>
          <select name="period" id="period" required>
              <option value="">-- Select Period --</option>
              <option value="recent">Last 30 minutes</option>
              <option value="today">Today (up to now)</option>
              <option value="week">Last 7 days</option>
              <option value="month">This month (up to today)</option>
              <option value="last_month">Last month</option>
          </select>    
  </div>

  <script>
    document.getElementById("period").addEventListener("change", function() {{
        let selectedPeriod = this.value;
        if (selectedPeriod) {{
            window.location.href = "/query/{meter_id}/" + selectedPeriod;
        }}
    }});
  </script>
  <br>
  <a href="/" style="display:inline-block; padding:10px 20px; 
     font-size:16px; color:white; background-color:#007bc2; 
     text-decoration:none; border-radius:5px; margin-top:20px;">
     Back to homepage
  </a>
</html>
"""

@app.route("/query/<meter_id>/recent",methods=["GET"])
def get_recent_consumption(meter_id):
    time1=(datetime.now()-timedelta(minutes=60)).strftime("%Y/%m/%d %H:%M")
    time2=(datetime.now()-timedelta(minutes=30)).strftime("%Y/%m/%d %H:%M")
    now_time=(datetime.now()).strftime("%Y/%m/%d %H:%M")
    today_data = requests.get(f"http://localhost:5000/vendor/meter_data").json()
    query_today = {}
    if meter_id in today_data:
        query_today = today_data[meter_id]
    for time,reading in query_today.items():
        if time1<=time<time2:
            reading1=reading
        if time2<=time<now_time:
            reading2=reading
    recent_consumption=round((reading2-reading1),2)
    back_button_html = f"""
    <br>
    <a href="/query/{meter_id}" style="display:inline-block; padding:10px 20px; 
       font-size:16px; color:white; background-color:#007bc2; 
       text-decoration:none; border-radius:5px; margin-top:20px;">
       Back to period selection
    </a>
    """
    return f"Your electricity usage for the last 30 minutes is {recent_consumption} kWh"+back_button_html

@app.route("/query/<meter_id>/today",methods=["GET"])
def get_today_consumption(meter_id):
    today_data = requests.get(f"http://localhost:5000/vendor/meter_data").json()
    query_today = {}
    if meter_id in today_data:
        query_today = today_data[meter_id]
    electricity_df = pd.DataFrame(list(query_today.items()), columns=["Date", "kWh"])
    electricity_df = electricity_df.sort_values(by="Date")
    today_total = electricity_df["kWh"].iloc[-1] - electricity_df["kWh"].iloc[0]
    today_total=round(today_total,2)
    plt.figure(figsize=(10, 6))
    sns.lineplot(x="Date", y="kWh", data=electricity_df, color="#007bc2")
    plt.title('Meter Reading Over Time')
    plt.xlabel('Date')
    plt.ylabel('kWh')
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode('utf-8')
    img_html = f'<img src="data:image/png;base64,{img_base64}" />'
    back_button_html = f"""
    <br>
    <a href="/query/{meter_id}" style="display:inline-block; padding:10px 20px; 
       font-size:16px; color:white; background-color:#007bc2; 
       text-decoration:none; border-radius:5px; margin-top:20px;">
       Back to period selection
    </a>
    """
    return f"<h2>Your total electricity consumption of today up to now is {today_total} kWh</h2>" \
           f"<h3>Detailed meter reading for every 30 minutes:</h3>" \
           f"{electricity_df.to_html(index=False)}" + img_html+back_button_html

@app.route("/query/<meter_id>/week",methods=["GET"])
def get_week_consumption(meter_id):
    today_date = datetime.today().strftime("%m-%d")
    start_date = (datetime.today() - timedelta(days=7)).strftime("%m-%d")
    year_month = datetime.today().strftime("%Y-%m")
    daily_data = requests.get(f"http://localhost:5000/vendor/{meter_id}/monthly_consumption/{year_month}").json()
    query_week={}
    for date,consumption in daily_data.items():
        if start_date <= date < today_date:
            query_week[date]=consumption
    electricity_df = pd.DataFrame(list(query_week.items()), columns=["Date", "kWh"])
    electricity_df = electricity_df.sort_values(by="Date")
    week_total = electricity_df["kWh"].sum()
    plt.figure(figsize=(10, 6))
    sns.lineplot(x="Date", y="kWh", data=electricity_df, color="#007bc2")
    plt.title('Electricity Consumption Over Time')
    plt.xlabel('Date')
    plt.ylabel('kWh')
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode('utf-8')
    img_html = f'<img src="data:image/png;base64,{img_base64}" />'
    back_button_html = f"""
    <br>
    <a href="/query/{meter_id}" style="display:inline-block; padding:10px 20px; 
       font-size:16px; color:white; background-color:#007bc2; 
       text-decoration:none; border-radius:5px; margin-top:20px;">
       Back to period selection
    </a>
    """
    return f"<h2>Your total electricity consumption during last seven days is {week_total} kWh</h2>" \
           f"<h3>Detailed electricity consumption for each day:</h3>" \
           f"{electricity_df.to_html(index=False)}" + img_html+back_button_html

@app.route("/query/<meter_id>/month",methods=["GET"])
def get_month_consumption(meter_id):
    year_month = datetime.today().strftime("%Y-%m")
    query_month = requests.get(f"http://localhost:5000/vendor/{meter_id}/monthly_consumption/{year_month}").json()
    electricity_df = pd.DataFrame(list(query_month.items()), columns=["Date", "kWh"])
    electricity_df = electricity_df.sort_values(by="Date")
    month_total = round(electricity_df["kWh"].sum(),2)
    plt.figure(figsize=(10, 6))
    sns.lineplot(x="Date", y="kWh", data=electricity_df, color="#007bc2")
    plt.title('Electricity Consumption Over Time')
    plt.xlabel('Date')
    plt.ylabel('kWh')
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode('utf-8')
    img_html = f'<img src="data:image/png;base64,{img_base64}" />'
    back_button_html = f"""
    <br>
    <a href="/query/{meter_id}" style="display:inline-block; padding:10px 20px; 
       font-size:16px; color:white; background-color:#007bc2; 
       text-decoration:none; border-radius:5px; margin-top:20px;">
       Back to period selection
    </a>
    """
    return f"<h2>Your total electricity consumption for the current month up to today is {month_total} kWh</h2>" \
           f"<h3>Detailed electricity consumption for each day:</h3>" \
           f"{electricity_df.to_html(index=False)}" + img_html+back_button_html

@app.route("/query/<meter_id>/last_month",methods=["GET"])
def get_last_month_consumption(meter_id):
    year_month = (datetime.today() - relativedelta(months=1)).strftime("%Y-%m")
    query_last_month = requests.get(f"http://localhost:5000/vendor/{meter_id}/monthly_consumption/{year_month}").json()
    electricity_df = pd.DataFrame(list(query_last_month.items()), columns=["Date", "kWh"])
    electricity_df = electricity_df.sort_values(by="Date")
    month_total = round(electricity_df["kWh"].sum(),2)
    plt.figure(figsize=(10, 6))
    sns.lineplot(x="Date", y="kWh", data=electricity_df, color="#007bc2")
    plt.title('Electricity Consumption Over Time')
    plt.xlabel('Date')
    plt.ylabel('kWh')
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode('utf-8')
    img_html = f'<img src="data:image/png;base64,{img_base64}" />'
    back_button_html = f"""
    <br>
    <a href="/query/{meter_id}" style="display:inline-block; padding:10px 20px; 
       font-size:16px; color:white; background-color:#007bc2; 
       text-decoration:none; border-radius:5px; margin-top:20px;">
       Back to period selection
    </a>
    """
    return f"<h2>Your total electricity consumption for last month is {month_total} kWh</h2>" \
           f"<h3>Detailed electricity consumption for each day:</h3>" \
           f"{electricity_df.to_html(index=False)}" + img_html+back_button_html
           
if __name__ == "__main__":
    vendor_thread = threading.Thread(target=Vendor().run, daemon=True)
    vendor_thread.start()
    app.run(port=5001)
