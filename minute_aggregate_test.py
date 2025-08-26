import time
import requests
import json
from datetime import datetime, timezone, timedelta

POLYGON_API_KEY = r"uOL9eSprjrGbjbbPC2qGEvvRTckJJ2y3"

def fetch_minute_aggregates_between(ticker, api_key, start_time, end_time):
    """
    Fetch minute aggregate price data from Polygon API between specified times.

    Args:
        ticker (str): The stock ticker symbol.
        api_key (str): Your Polygon API key.
        start_time (str): The start time in ISO 8601 format.
        end_time (str): The end time in ISO 8601 format.

    Returns:
        dict: The JSON response from the Polygon API.
    """
    # Convert PST times to UTC timestamps in milliseconds
    pst = timezone(timedelta(hours=-8))
    start_datetime = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pst)
    end_datetime = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pst)

    start_timestamp = int(start_datetime.timestamp() * 1000)
    end_timestamp = int(end_datetime.timestamp() * 1000)

    # Polygon API endpoint for aggregates
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{start_timestamp}/{end_timestamp}?&apiKey={POLYGON_API_KEY}"


    # Make the request
    response = requests.get(url)

    # Raise an error if the request failed
    response.raise_for_status()

    return response.json()

# Example usage
api_key = "your_polygon_api_key"
ticker = "AAPL"  # Example ticker
start_time = "2024-12-26T12:15:00"
end_time = "2024-12-26T12:45:00"

try:
    data = fetch_minute_aggregates_between(ticker, api_key, start_time, end_time)
    # Pretty-print the JSON response
    print(json.dumps(data, indent=4))
except Exception as e:
    print(f"An error occurred: {e}")
