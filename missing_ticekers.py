import os
import pandas as pd

# Paths
ticker_csv_path = r'Q:\Project_BOLT\Data_Formation_Scripts\Ticker_Classifications\tickers.csv'
data_folder = r'Q:\Project_BOLT\Data_Formation_Scripts\Polygon_Stock_Scripts\Price_data'

# Read tickers from the original CSV
tickers = pd.read_csv(ticker_csv_path).iloc[:, 0].tolist()  # Adjust column index if needed

# Get tickers from the folder
files_in_folder = os.listdir(data_folder)
tickers_in_folder = [file.replace('.csv', '') for file in files_in_folder if file.endswith('.csv')]

# Find missing tickers
missing_tickers = set(tickers) - set(tickers_in_folder)

print(f"Missing tickers: {missing_tickers}")