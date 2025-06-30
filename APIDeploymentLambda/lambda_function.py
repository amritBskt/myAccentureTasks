import requests
import csv
import os
import time
import boto3

from Exceptions.my_exceptions import RedirectionError, ClientError, ServerError, UnexpectedError

def fetch_weather(key, city="Bengaluru", time_out=3, retries=3, delay=2):
    url = "https://api.openweathermap.org/data/2.5/weather"
    headers = {"Content-Type": "application/json"}
    payload = {
        "q": city,
        "appid": key
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=payload,
                timeout=time_out
            )

            if "charset=utf-8" not in response.headers.get("Content-Type", ""):
                raise Exception("Invalid encoding format")

            status = response.status_code
            data = response.json()

            if 200 <= status < 300:
                if "main" in data:
                    weather = data['main']
                    return {
                        "city": city,
                        "weather": data['weather'][0]['description'],
                        **weather
                    }
                else:
                    if attempt == retries:
                        raise Exception("Weather data not received")
                    time.sleep(delay)

            elif 300 <= status < 400:
                if attempt == retries:
                    raise RedirectionError(f"{status} {data.get('message', 'No message')}")
                time.sleep(delay)

            elif 400 <= status < 500:
                raise ClientError(f"{status} {data.get('message', 'No message')}")

            elif 500 <= status < 600:
                if attempt == retries:
                    raise ServerError(f"{status} {data.get('message', 'No message')}")
                time.sleep(delay)

            else:
                if attempt == retries:
                    raise UnexpectedError(f"{status} {data.get('message', 'No message')}")
                time.sleep(delay)

        except requests.exceptions.Timeout:
            if attempt == retries:
                raise
            time.sleep(delay)

        except requests.exceptions.RequestException:
            if attempt == retries:
                raise Exception(f'Network error after maximum retries: {retries}')
            time.sleep(delay)

def write_to_csv(weather_data, filename="/tmp/weather.csv"):
    file_exists = os.path.exists(filename)
    with open(filename, 
              mode="a" if file_exists else "w",
              encoding="utf-8",
              newline="") as file:
        writer = csv.DictWriter(file, fieldnames=weather_data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(weather_data)

def upload_to_s3(local_path, bucket, key):
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, key)

def lambda_handler(event, context):
    # Get API key from environment variable
    key = os.environ.get("WEATHER_API_KEY")
    if not key:
        return {
            "statusCode": 500,
            "body": "API key not set in environment variable WEATHER_API_KEY"
        }
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        return {
            "statusCode": 500,
            "body": "S3 bucket not set in environment variable S3_BUCKET"
        }
    city = event.get("city", "Bengaluru")
    try:
        weather_data = fetch_weather(key=key, city=city, time_out=3)
        local_csv_path = "/tmp/weather.csv"
        write_to_csv(weather_data, filename=local_csv_path)
        s3_key = f"weather.csv"
        upload_to_s3(local_csv_path, bucket, s3_key)
        return {
            "statusCode": 200,
            "body": f"Weather data for {city} saved to S3://{bucket}/{s3_key}",
            "data": weather_data,
            "s3_path": f"s3://{bucket}/{s3_key}"
        }
    except requests.exceptions.Timeout:
        return {
            "statusCode": 504,
            "body": "API call timed out"
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }
