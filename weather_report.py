import requests
import mysql.connector
from datetime import datetime
import schedule
import time

# get you api from https://openweathermap.org/api

# --- Configuration ---
API_KEY = 'your _api_key_here'

# You change the cities as per your requirement
cities = [
        "Ahmedabad",
        "Surat",
        "Vadodara",
        "Rajkot",
        "Bhavnagar",
        "Jamnagar",
        "Junagadh",
        "Nadiad",
        "Gandhinagar",
        "Morbi",
        "Bharuch",
        "Navsari",
        "Porbandar",
        "Veraval",
        "Surendranagar",
        "Anand",
        "Gandhidham",
        "Mehsana",
        "Bhuj",
        "Godhra",
        "Palanpur",
        "Vejalpur",
        "Patan",
        "Kalol",
        "Ghatlodiya",
        "Jetpur",
        "Vav",
        "Tharad"
    ]

# --- Data Collection Function ---
def fetch_weather_data():
    print(f"\nRunning weather fetch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root@123',
        database='weather_project'
    )
    cursor = conn.cursor()

    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            city_name = data['name']
            temperature = data['main']['temp']
            humidity = data['main']['humidity']
            pressure = data['main']['pressure']
            weather_desc = data['weather'][0]['description']
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            latitude = data['coord']['lat']
            longitude = data['coord']['lon']


            insert_query = """
            INSERT INTO weather_data (city, temperature, humidity, pressure, weather_desc, timestamp, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (city_name, temperature, humidity, pressure, weather_desc, timestamp, latitude, longitude)

            cursor.execute(insert_query, values)
            conn.commit()

            print(f"✅ Inserted data for {city_name}")
        else:
            print(f"❌ Failed for {city}: {response.status_code}")

    cursor.close()
    conn.close()

# --- Scheduling ---
schedule.every(2).minutes.do(fetch_weather_data)

print("✅ Scheduler started... Running every 2 minutes.")
fetch_weather_data()  # Run immediately once

while True:
    schedule.run_pending()
    time.sleep(1)
