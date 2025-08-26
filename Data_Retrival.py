import requests
import pandas as pd
import random
from io import StringIO
from dictionaries import gics_keywords, companies
from concurrent.futures import ThreadPoolExecutor
import threading
import os

all_keywords = [word for keywords in gics_keywords.values() for word in keywords]
all_keywords.extend(companies)

# List of dates to process
def get_dates_in_range(start_date_str, end_date_str):
    from datetime import datetime, timedelta
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")
    return [(start_date + timedelta(days=i)).strftime("%Y%m%d") for i in range((end_date - start_date).days + 1)]

# Dates to process
dates = get_dates_in_range('20130401', '20241114')
dates.reverse()

# Random user-agent generation function
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

# Function to retrieve volume and sentiment for a specific date and keyword
def retrieve_vol_and_sent(date, word):
    url = f"https://api.gdeltproject.org/api/v2/doc/doc?query='{word}'&mode=tonechart&format=csv&STARTDATETIME={date}000000&ENDDATETIME={date}235959"
    response = requests.get(url=url, headers=get_header())
    print(response.status_code)
    if response.status_code != 200:
        print(f"Failed to retrieve data for {word} on {date}: {response.status_code}")
        return 0, 0  # Return zeros on failure
    
    try:
        word_csv = pd.read_csv(StringIO(response.text))
        volume = word_csv.iloc[:, 1].sum()
        avg_sentiment = word_csv.iloc[:, 0].sum() / volume if volume != 0 else 0
        return round(volume, 5), round(avg_sentiment, 5)
    except Exception as e:
        print(f"Error processing CSV for {word} on {date}: {e} -----handled")
        return 0, 0  # Return zeros on error

# Function to update the DataFrame with the new data for a specific date and word
def update_df(df, date, word, vol, avg_sentiment):
    row_index = dates.index(date) + 1  # Adjust for header row
    col_index_vol = all_keywords.index(word) * 2 + 1  # Volume is in an odd column
    col_index_sent = all_keywords.index(word) * 2 + 2  # Sentiment is in the next column
    
    df.iloc[row_index, col_index_vol] = vol
    df.iloc[row_index, col_index_sent] = avg_sentiment
    print(f"Updated data for {date} and word: {word}")
    
    return df

# Worker function to fetch data for a specific date and word, and update the DataFrame
def worker_for_api(date, word, df):
    vol, avg_sentiment = retrieve_vol_and_sent(date, word)
    df = update_df(df, date, word, vol, avg_sentiment)
    return df

# Graceful shutdown handler
def save_to_csv(df, path):
    try:
        df.to_csv(path, index=False)
        print("Data successfully saved to CSV.")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def save_batch(df, path, batch_index):
    """Save a batch of rows to a CSV file."""
    batch_path = os.path.join(path, f"batch_{batch_index}.csv")
    try:
        df.to_csv(batch_path, index=False)
        print(f"Batch {batch_index} successfully saved to {batch_path}.")
    except Exception as e:
        print(f"Error saving batch {batch_index} to CSV: {e}")

def main():
    folder_path = r"Q:\Project_BOLT\Data_Storage\Key_Word_Data"
    os.makedirs(folder_path, exist_ok=True)  # Ensure the folder exists

    # Initialize an empty DataFrame
    columns = [f"{word}_volume" for word in all_keywords] + [f"{word}_sentiment" for word in all_keywords]
    df = pd.DataFrame(columns=["date"] + columns)
    df["date"] = dates  # Add dates to the first column

    batch_rows = 1
    batch_index = 1
    current_batch = pd.DataFrame(columns=df.columns)

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i, date in enumerate(dates):
            futures = []
            for word in all_keywords:
                futures.append(executor.submit(worker_for_api, date, word, df))
                
            # Wait for all threads to finish for this date
            for future in futures:
                current_batch = pd.concat([current_batch, future.result()], ignore_index=True)
            
            # If the current batch is full, save it and reset
            if len(current_batch) >= batch_rows:
                save_batch(current_batch, folder_path, batch_index)
                batch_index += 1
                current_batch = pd.DataFrame(columns=df.columns)  # Reset the batch
        
    save_to_csv(df, os.path.join(folder_path, "final_data.csv"))  # Save the final data to CSV

if __name__ == "__main__":
    main()
