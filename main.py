import json
from datetime import datetime
import pandas as pd
import requests

city_name = "Lagos"
base_url = "https://api.openweathermap.org/data/2.5/weather?q="

try:
    with open("requirements.txt", 'r') as f:
        api_key = f.read()
except FileNotFoundError:
    print("Error: requirements.txt file not found.")
    exit(1)

full_url = base_url + city_name + "&APPID=" + api_key
# r = requests.get(full_url)
# data = r.json()
# print(data)

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def etl_weather_data(url):
    try:
        r = requests.get(full_url)
        r.raise_for_status()
        data = r.json()

        city = data["name"]
        weather_description = data["weather"][0]['description']
        temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
        feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
        min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
        max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
        pressure = data["main"]["pressure"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
        sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
        sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

        transformed_data = {"City": city,
                            "Description": weather_description,
                            "Temperature (F)": temp_fahrenheit,
                            "Feels Like (F)": feels_like_fahrenheit,
                            "Minimum Temp (F)": min_temp_fahrenheit,
                            "Maximum Temp (F)": max_temp_fahrenheit,
                            "Pressure": pressure,
                            "Humidity": humidity,
                            "Wind Speed": wind_speed,
                            "Time of Record": time_of_record,
                            "Sunrise (Local Time)": sunrise_time,
                            "Sunset (Local Time)": sunset_time
                            }

        transformed_data_list = [transformed_data]
        df_data = pd.DataFrame(transformed_data_list)

        df_data.to_csv("current_weather_data_lagos.csv", index=False)
        print("Weather data successfully retrieved and saved to 'current_weather_data_lagos.csv'.")

    except requests.exceptions.RequestException as e:
        print(f"Error in making the request: {str(e)}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {str(e)}")
    except KeyError as e:
        print(f"Error: Key not found in response JSON: {str(e)}")

if __name__ == '__main__':
    etl_weather_data(full_url)
