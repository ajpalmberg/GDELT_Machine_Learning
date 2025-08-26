import os
import csv
import time
import random
from datetime import datetime, timedelta
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
import requests
import pandas as pd
from io import StringIO
from copy import deepcopy

from dictionaries import gics_keywords, companies, geopolitical_topics, topics, industry_synonyms_lowercase


all_keywords = list(word for keywords in gics_keywords.values() for word in keywords)
all_keywords = all_keywords + list(word for keywords in geopolitical_topics.values() for word in keywords)
all_keywords = all_keywords + list(word for keywords in topics.values() for word in keywords)
all_keywords = all_keywords + list(word for keywords in industry_synonyms_lowercase.values() for word in keywords)
for company in companies: all_keywords.append(company)

all_keywords = list(word.lower() for word in all_keywords)
all_keywords = list(dict.fromkeys(all_keywords))
output_folder = r"Q:\Project_BOLT\Data_Storage\Key_Word_Data"


def get_header():
    user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-G960F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/602.3.12 (KHTML, like Gecko) Version/10.0.3 Safari/602.3.12",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/18.19041",
    "Mozilla/5.0 (Linux; Android 8.0.0; SM-G950F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
    "Mozilla/5.0 (Linux; U; Android 4.4.2; en-us; Nexus 5 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 7.0; Nexus 6P Build/NBD90X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:64.0) Gecko/20100101 Firefox/64.0"
    ]
    return {"User-Agent": random.choice(user_agents)}

def retrieve_vol_and_sent(date, word, csv_dict, lock):
    print(f'{all_keywords.index(word)}---{len(all_keywords)}')

    url = f"https://api.gdeltproject.org/api/v2/doc/doc?query='{word}'&mode=tonechart&format=csv&STARTDATETIME={date}000000&ENDDATETIME={date}235959"
    response = requests.get(url=url, headers=get_header())
    volume = 0
    avg_sentiment = 0
    avg_distribution = 0

    if response.status_code != 200:
        print(f"Failed to retrieve data for {word} on {date}: {response.status_code}")    
    try:
        word_csv = pd.read_csv(StringIO(response.text))
        volume = word_csv.iloc[:, 1].sum()
        avg_sentiment = word_csv.iloc[:, 0].sum() / volume if volume != 0 else 0
        if volume != 0:
            abs_differences = abs(word_csv.iloc[:, 0] - avg_sentiment)
            weighted_differences = abs_differences * word_csv.iloc[:, 1]
            avg_distribution = weighted_differences.sum() / volume
    except Exception as e:
        print(f"Error processing CSV for {word} on {date}: {e} -----handled")
    
    with lock:
        csv_dict[f"{word}_volume"] = volume
        csv_dict[f"{word}_sentiment"] = avg_sentiment
        csv_dict[f"{word}_distribution"] = avg_distribution

def date_maker(date):
    words = deepcopy(all_keywords)
    worker_dict = {}

    lock = Lock()
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = []

        while words:
            word = words.pop(0)
            futures.append(executor.submit(retrieve_vol_and_sent, date, word, worker_dict, lock))
    
        for future in futures:
            future.result()
    
    file_path = os.path.join(output_folder, f'{date}.csv')
    with open(file_path, mode='w', newline='') as file:
        fieldnames = [f"{word}_{metric}" for word in all_keywords for metric in ["volume", "sentiment", "distribution"]]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(worker_dict)
    
def initialize_five_dates(list_of_dates):
    dates = list_of_dates
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(date_maker, dates[id_number]) for id_number in range(len(dates))]

        for future in futures:
            future.result()

def get_dates_in_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")
    return [(start_date + timedelta(days=i)).strftime("%Y%m%d") for i in range((end_date - start_date).days + 1)]

def initialize_batches(dates, batch_size=5):
    for i in range(0, len(dates), batch_size):
        current_batch = dates[i:i + batch_size]
        print(current_batch)
        initialize_five_dates(current_batch)
        time.sleep(1)  # Avoid overwhelming the API

dates = get_dates_in_range("20130401", "20171218")
dates.reverse()
initialize_batches(dates)