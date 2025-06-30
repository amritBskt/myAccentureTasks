import requests
import csv
import os
import time
from Exceptions.my_exceptions import *
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.getenv("API_KEY")

def fetch_weather(key=api_key, city="Bengaluru", time_out=3, retries=3, delay=2):
    url = "https://api.openweathermap.org/data/2.5/weather/"
    headers = {"Content-Type": "application/json"}
    payload = {
        "q": city,
        "appid": f"{key}"
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                url,
                headers=headers,
                params=payload,
                timeout=time_out
            )

            if "charset=utf-8" not in response.headers.get("Content-Type", ""):
                raise Exception("Invalid encoding format")

            status = response.status_code
            data = response.json()
            # print(data)
            if 200 <= status < 300:
                if "main" in data:
                    weather = data['main']
                    print(f"{status} success")
                    return {"city": f"{city}", "weather": f"{data['weather'][0]['description']}", **weather}
                else:
                    print(f"Weather data missing 'main' key, attempt {attempt}/{retries}")
                    if attempt == retries:
                        raise Exception("Weather data not received")
                    time.sleep(delay)

            elif 300 <= status < 400:
                print(f"{status} Redirection Error, attempt {attempt}/{retries}")
                if attempt == retries:
                    raise RedirectionError(f"{status} {data.get('message', 'No message')}")
                time.sleep(delay)

            elif 400 <= status < 500:
                raise ClientError(f"{status} {data.get('message', 'No message')}")

            elif 500 <= status < 600:
                print(f"{status} Server Error, attempt {attempt}/{retries}")
                if attempt == retries:
                    raise ServerError(f"{status} {data.get('message', 'No message')}")
                time.sleep(delay)

            else:
                print(f"{status} Unexpected Error, attempt {attempt}/{retries}")
                if attempt == retries:
                    raise UnexpectedError(f"{status} {data.get('message', 'No message')}")
                time.sleep(delay)

        except requests.exceptions.Timeout:
            print(f"Timeout occurred, attempt {attempt}/{retries}")
            if attempt == retries:
                raise
            time.sleep(delay)

        except requests.exceptions.RequestException:
            print(f"Network error: attempt {attempt}/{retries}")
            if attempt == retries:
                raise Exception(f'Network error: after maximum retries: {retries}')
            time.sleep(delay)
    
def main():
    try:
        weather_data = fetch_weather(key=api_key, city="Bengaluru", time_out=3)

        # print(type(weather_data))
        # print(weather_data)

        filename = "weather.csv"
        file_exists = os.path.exists(filename)
        with open(filename, 
                    mode="a" if file_exists else "w",
                    encoding="utf-8",
                    newline="") as file:
            writer = csv.DictWriter(file, fieldnames=weather_data.keys())
            
            if not file_exists:
                writer.writeheader()
            writer.writerow(weather_data)

    except requests.exceptions.Timeout:
        print("API call timed out")
    except Exception as e:
        print(str(e))
        

if __name__=="__main__":
    main()

