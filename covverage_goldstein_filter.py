import os
import pandas as pd
from datetime import datetime, timedelta
import time  # For measuring time

def filter_and_copy_csv(file_name, total_rows):
    # Define the input and output folder paths inside the function
    input_folder = "Q:/Project_BOLT/Data_Storage/GDELT_Important_Events_Filtered"  # Path to input folder
    output_folder = "Q:/Project_BOLT/Data_Storage/GDELT_Event_Viewing_Goldstein_Filter"  # Path to output folder
    
    # Construct the full input file path
    input_file_path = os.path.join(input_folder, file_name)
    
    # Check if the input file exists
    if not os.path.isfile(input_file_path):
        print(f"Error: File not found - {input_file_path}")
        return total_rows
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(input_file_path, low_memory=False)
    
    # Apply the filtering conditions:
    # Column 33 (0-indexed, so it's column 34 in human-readable list)
    condition_1 = df.iloc[:, 33] >= 4
    # Column 30 (0-indexed, so it's column 31 in human-readable list)
    condition_2 = abs(df.iloc[:, 30]) > 2
    
    # Remove rows where both column 12 and 22 are missing (NaN)
    condition_3 = df.iloc[:, 12].notna() | df.iloc[:, 22].notna()
    
    # Combine the conditions to filter rows
    filtered_df = df[condition_1 & condition_2 & condition_3]
    
    # Print the number of rows in the filtered DataFrame
    print(f"Number of rows after filtering: {filtered_df.shape[0]}")
    
    # Update total row count
    total_rows += filtered_df.shape[0]
    
    # Print the total rows divided by 1000 for each file
    print(f"Total rows so far (divided by 1000): {total_rows / 1000:.2f}")
    
    # Define the output file path
    output_file_path = os.path.join(output_folder, file_name)
    
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Save the filtered DataFrame to the output folder with the same name
    filtered_df.to_csv(output_file_path, index=False)
    
    return total_rows

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

def track_average_time(dates):
    total_time = 0  # To store total time taken
    total_rows = 0  # To store total row count across all files
    for index, date in enumerate(dates):
        start_time = time.time()  # Record the start time
        total_rows = filter_and_copy_csv(f"{date}.csv", total_rows)  # Update total row count
        end_time = time.time()  # Record the end time
        time_taken = end_time - start_time  # Calculate time for this file
        total_time += time_taken
        
        average_time = total_time / (index + 1)
        files_left = len(dates) - index

        # Estimate time left
        estimated_time_left = average_time * files_left
        
        # Format and display the estimated time left
        formatted_time_left = format_seconds(estimated_time_left)
        if (index +1) % 10 == 0:
            print(f"Finished Date: {date} ({index + 1} out of {len(dates)})")
            print(f"Estimated Time Left: {formatted_time_left}")

# Example usage:
dates = generate_dates("20130401", "20241114")

# Track the average time while processing all dates
track_average_time(dates)
