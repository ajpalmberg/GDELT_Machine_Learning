import os
import pandas as pd
from datetime import datetime, timedelta
import time  # For measuring time

def create_shifting_window_chunks(file_name):
    # Define the input and output folder paths
    input_folder = "Q:\Project_BOLT\Data_Storage\GDELT_Event_Viewing_Goldstein_Filter"  # Replace with your input folder path
    output_folder = "Q:\Project_BOLT\Data_Storage\GDELT_Chunks"  # Replace with your output folder path

    # Construct the full path to the input file
    input_file_path = os.path.join(input_folder, file_name)
    
    # Check if the input file exists
    if not os.path.isfile(input_file_path):
        print(f"Error: File not found - {input_file_path}")
        return
    
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(input_file_path, low_memory=False)
    
    # Create a subfolder in the output folder with the original file name (without extension)
    base_name = os.path.splitext(file_name)[0]
    output_subfolder = os.path.join(output_folder, base_name)
    os.makedirs(output_subfolder, exist_ok=True)
    
    # Parameters for shifting window
    window_size = 1000
    step_size = 250
    num_chunks = (len(df) - window_size) // step_size + 1  # Calculate the number of chunks

    # Create chunks with a shifting window
    for i in range(num_chunks):
        start_row = i * step_size
        end_row = start_row + window_size
        chunk = df.iloc[start_row:end_row]
        
        # Define the chunk file name
        chunk_file_name = f"{base_name}-{i + 1}.csv"
        chunk_file_path = os.path.join(output_subfolder, chunk_file_name)
        
        # Save the chunk as a new CSV file
        chunk.to_csv(chunk_file_path, index=False)

def generate_dates(start_date, end_date):
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates

# Function to convert seconds into a readable time format (HH:MM:SS)
def format_seconds(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

# Function to track average time
def track_average_time(dates):
    total_time = 0  # To store total time taken
    for index, date in enumerate(dates):
        start_time = time.time()  # Record the start time
        create_shifting_window_chunks(f"{date}.csv")
        end_time = time.time()  # Record the end time
        time_taken = end_time - start_time  # Calculate time for this file
        total_time += time_taken
        
        average_time = total_time / (index + 1)
        files_left = len(dates) - index

        # Estimate time left
        estimated_time_left = average_time * files_left
        
        # Format and display the estimated time left
        formatted_time_left = format_seconds(estimated_time_left)
        
        print(f"Finished Date: {date} ({index + 1} out of {len(dates)})")
        print(f"Estimated Time Left: {formatted_time_left}")

dates = generate_dates("20130401", "20241114")

# Track the average time while processing all dates
track_average_time(dates)
