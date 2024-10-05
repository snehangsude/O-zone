# type: ignore
import time
import json
import pathlib
import requests 
from datetime import datetime
from kafka import KafkaProducer 
import uuid
from modules.YamlReader import YamlReader

# Predominatly for dev-testing
import sys

current_path = pathlib.Path(__file__)
config = YamlReader.read_config(f"{current_path.parents[1]}/config/config.yml")

ZIP_CODES = config['postal_codes']
API_KEY = config["openWeatherData_API"]
OPENWEATHER_ENDPOINT = config["openWeatherData_endpoint"]


def get_coordinates(zip_code, country_code):
    url = f"{OPENWEATHER_ENDPOINT}/geo/1.0/zip"
    parameters = {
        'zip': f"{zip_code},{country_code}",
        'appid': API_KEY
    }
    response = requests.get(url, params=parameters) 
    return response.json()


def get_openWeather_airPollution_data():
    
    url = f"{OPENWEATHER_ENDPOINT}/data/2.5/air_pollution"
    current_datetime = datetime.now().replace(second=0, microsecond=0)
    
    timeblock_data = []
    code_pairs = [(zip_code, country_code) for country_code, zip_codes in ZIP_CODES.items() for zip_code in zip_codes]


    for idx, val in code_pairs:
        parameters = {key: value for key, value in get_coordinates(idx, val).items() if key in ['lat', 'lon', 'name']}
        parameters['appid'] = API_KEY
        openWeather_data = requests.get(url=url, params=parameters).json()
        normalized_data = {
            'message_id' : uuid.uuid4().hex,
            'date': current_datetime.strftime('%Y-%m-%d'),
            'time': current_datetime.strftime('%H:%M'),    
            'lat': openWeather_data['coord']['lat'],
            'lon': openWeather_data['coord']['lon'],
            'city': parameters['name'],
            'aqi': openWeather_data['list'][0]['main']['aqi'],
            'carbon_monoxide': openWeather_data['list'][0]['components']['co'],
            'nitrogen_monoxide': openWeather_data['list'][0]['components']['no'],
            'nitrogen_dioxide': openWeather_data['list'][0]['components']['no2'],
            'ozone': openWeather_data['list'][0]['components']['o3'],
            'sulphur_dioxide': openWeather_data['list'][0]['components']['so2'],
            'fine_particles2.5': openWeather_data['list'][0]['components']['pm2_5'],
            'coarse_particles10': openWeather_data['list'][0]['components']['pm10'],
            'ammonia': openWeather_data['list'][0]['components']['nh3']
        }
        timeblock_data.append(normalized_data)

    return timeblock_data



def main():

    kafka_producer = KafkaProducer(
          bootstrap_servers = "localhost:9092",
          value_serializer = lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        airPollution_data = get_openWeather_airPollution_data()
        for messages in airPollution_data:
            kafka_producer.send(
                topic='openWeather_pollution_data',
                value=messages
            )
            print(f"Produced: {messages}\n")
            kafka_producer.flush()
        
        time.sleep(10)

if __name__ == "__main__":
    main()