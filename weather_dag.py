from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd

# Function to convert temperature from Kelvin to Fahrenheit
def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

# Function to transform and load data
def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
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

    aws_credentials = {"key": "ASIA2DEQITH3FNDH4F43", "secret": "WzbYOKveAhe90aTWEhTCHnmeXsRU9kBVblG8LOUT", "token": "FwoGZXIvYXdzEJb//////////wEaDAK7FG8z6wMyzUXR7yJqoTFYtEzu9C1Rv5SR4C06Yfg7C5XiMI0FTpLChm9/ZFX2MxhnK+OTOLtNDJ7zF1fdgexchSI7Y9Iq3vLey31rl0QDw2RuXUEOZDYEt7sSYveiSw0Sl9VLqQM5wHCepqduHP2NtBrdLOyWqyiputynBjIo1gQSXaxuwfwlH5Y2480u3ka0ZtifDB+vBGr/C69/9mdHMonr3aZ3uQ=="}

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_Lagos_' + dt_string
    
    # Save data to S3 bucket
    df_data.to_csv(f"s3://openweather009/{dt_string}.csv", index=False, storage_options=aws_credentials)

# Define DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# Create the DAG
with DAG('weather_dag',
        default_args=default_args,
        schedule_interval='@daily',  # Adjust the schedule as needed
        catchup=False) as dag:

    # Task to check if the weather API is ready
    is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Lagos&APPID=0cc2f0b1b27ea1c596446743aaedc014'
    )

    # Task to extract weather data
    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Lagos&APPID=0cc2f0b1b27ea1c596446743aaedc014',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    # Task to transform and load weather data
    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data
    )

# Define task dependencies
is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
