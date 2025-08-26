import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import os
import csv

def get_dates_in_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")
    return [
        (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end_date - start_date).days + 1)
        if (start_date + timedelta(days=i)).weekday() < 5  # Monday to Friday
    ]

def get_openclose(ticker, dates, dataframe, lock):
    for date in dates:
        url = f"https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey=uOL9eSprjrGbjbbPC2qGEvvRTckJJ2y3"

        try:
            response = requests.get(url)
            response.raise_for_status()
            try:
                json_data = response.json()
            except ValueError:
                print(f"Malformed JSON for {ticker} on {date}: {response.text}")
                continue

            new_row = {key: json_data.get(key, None) for key in ["from", "high", "low", "open", "close", "preMarket", "afterHours", "volume"]}

            with lock:
                dataframe.append(new_row)
        except requests.exceptions.RequestException as e:
            print(f"An error occurred for {ticker} on {date}: {e}")

def make_stock_sheet(ticker, dates):
    directory = r"Q:\\Project_BOLT\\Data_Formation_Scripts\\Polygon_Stock_Scripts\\Price_data"

    df = []
    lock = Lock()

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.submit(get_openclose, ticker, dates, dataframe=df, lock=lock)

    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, f'{ticker}.csv')

    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        if df:
            fieldnames = df[0].keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(df)
        else:
            print(f"No data available for {ticker}.")

def load_tickers_from_csv(file_path, start_row=0):
    tickers = []
    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row:
                    print(row)
                    if reader.line_num > start_row:
                        tickers.append(row[0].strip())
    except Exception as e:
        print(f"Error reading tickers from file: {e}")
    return tickers

def main(tickers, start_date, end_date, start_row=0):
    dates = get_dates_in_range(start_date, end_date)

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(make_stock_sheet, ticker, dates) for ticker in tickers]

        for future in futures:
            future.result()

main(load_tickers_from_csv(r"Q:\Project_BOLT\Data_Formation_Scripts\Ticker_Classifications\tickers.csv", start_row=0), "20200101", "20241130")


#response = requests.get("https://api.polygon.io/v1/open-close/META/2020-01-02?adjusted=true&apiKey=uOL9eSprjrGbjbbPC2qGEvvRTckJJ2y3")
#print(response.status_code)
#print(response.text)
