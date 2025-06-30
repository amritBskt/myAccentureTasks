import requests
import csv
import os
import random

def fetch_stocks(num_stocks=1, time_out=3):
    url = "https://api.freeapi.app/api/v1/public/stocks"

    page = random.randint(1,int(1000/num_stocks))

    querystring = {"page":f"{page}",
                   "limit":f"{num_stocks}",
                   "inc":"Symbol,Name,MarketCap,CurrentPrice"}
    headers = {"accept": "application/json"}

    response = requests.get(url, 
                            headers = headers, 
                            params = querystring, 
                            timeout = time_out)
    

    if "charset=utf-8" not in response.headers["Content-type"]:
        raise Exception("Invalid encoding format")

    response = response.json()
    
    if response["success"] and "data" in response:
        data = response["data"]["data"]
        # symbol = data["Symbol"]
        # name = data["Name"]
        # marketCap = data["MarketCap"]
        # currentPrice = data["CurrentPrice"]
        return data
    else:
        raise Exception("Failed to fetch stock data")
    
def main():
    try:
        stock_data = fetch_stocks(num_stocks=5, time_out=1)

        filename = "stocks.csv"
        file_exists = os.path.exists(filename)

        print(stock_data)
        with open(filename, 
                    mode="a" if file_exists else "w",
                    encoding="utf-8",
                    newline="") as file:
            writer = csv.DictWriter(file, fieldnames=stock_data[0].keys())
            
            if not file_exists:
                writer.writeheader()
            writer.writerows(stock_data)

    except requests.exceptions.Timeout:
        print("API call timed out")
    except Exception as e:
        print("Error: ", str(e))
        

if __name__=="__main__":
    main()
