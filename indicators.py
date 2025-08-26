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

def get_indicators(ticker, date, dataframe, lock):
    session = requests.Session()
    try:
        new_row = {"Date": date, "SMA_3_day": 0, "SMA_7_day": 0, "SMA_30_day": 0, "SMA_365_day": 0,
                   "EMA_3_day": 0, "EMA_7_day": 0, "EMA_30_day": 0, "EMA_365_day": 0,
                   "MACD_value": 0, "MACD_signal_line": 0, "MACD_histogram": 0, "RSI_value": 0}

        for i in ["ema", "sma"]:
            for number in [3, 7, 30, 365]:
                try:
                    response = session.get(f"https://api.polygon.io/v1/indicators/{i}/{ticker}?timespan=day&adjusted=true&window={number}&series_type=close&order=desc&limit=1&timestamp={date}&apiKey=uOL9eSprjrGbjbbPC2qGEvvRTckJJ2y3")
                    response.raise_for_status()
                    data = response.json().get("results", {}).get("values", [{}])
                    if data and len(data) > 0:
                        new_row[f"{i.upper()}_{number}_day"] = data[0].get("value", 0)
                except Exception as e:
                    print(f"Error fetching {i.upper()}_{number}_day for {ticker} on {date}: {e}")

        try:
            response = session.get(f"https://api.polygon.io/v1/indicators/macd/{ticker}?timespan=day&adjusted=true&short_window=12&long_window=26&signal_window=9&series_type=close&order=desc&limit=1&timestamp={date}&apiKey=uOL9eSprjrGbjbbPC2qGEvvRTckJJ2y3")
            response.raise_for_status()
            json_response = response.json().get("results", {}).get("values", [{}])
            if json_response and len(json_response) > 0:
                new_row["MACD_histogram"] = json_response[0].get("histogram", 0)
                new_row["MACD_signal_line"] = json_response[0].get("signal", 0)
                new_row["MACD_value"] = json_response[0].get("value", 0)
        except Exception as e:
            print(f"Error fetching MACD for {ticker} on {date}: {e}")

        try:
            response = session.get(f"https://api.polygon.io/v1/indicators/rsi/{ticker}?timespan=day&adjusted=true&window=7&series_type=close&order=desc&limit=1&timestamp={date}&apiKey=uOL9eSprjrGbjbbPC2qGEvvRTckJJ2y3")
            response.raise_for_status()
            json_response = response.json().get("results", {}).get("values", [{}])
            if json_response and len(json_response) > 0:
                new_row["RSI_value"] = json_response[0].get("value", 0)
        except Exception as e:
            print(f"Error fetching RSI for {ticker} on {date}: {e}")

        with lock:
            dataframe.append(new_row)
    except Exception as e:
        print(f"An error occurred for {ticker} on {date}: {e}")

def make_stock_sheet(ticker, dates):
    print(f"Making sheet for {ticker}")
    directory = r"Q:\Project_BOLT\Data_Formation_Scripts\Polygon_Stock_Indicators\Stock_Indicator_Data"

    df = []
    lock = Lock()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_indicators, ticker=ticker, dataframe = df, date=date, lock=lock) for date in dates]

        for future in futures:
            future.result()
    
    print(df)

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
                    if reader.line_num > start_row:
                        tickers.append(row[0].strip())
    except Exception as e:
        print(f"Error reading tickers from file: {e}")
    return tickers

def main(tickers, start_date, end_date, start_row=0):
    dates = get_dates_in_range(start_date, end_date)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(make_stock_sheet, ticker, dates) for ticker in tickers]

        for future in futures:
            future.result()

main(load_tickers_from_csv(r"Q:\Project_BOLT\Data_Formation_Scripts\Ticker_Classifications\tickers.csv"), "20200101", "20241130")