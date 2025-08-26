import os
import pandas as pd
from datetime import datetime, timedelta

def iterate_csv(file_name=None):
    input_folder = r"Q:\Project_BOLT\Data_Storage\GDELT_News_Filtered_To_10_Countries"  # Path to the folder with original files
    output_folder = r"Q:\Project_BOLT\Data_Storage\GDELT_Important_Events_Filtered"  # Path to the folder where you want to save the filtered files
    
    # Start the timer
    start_time = datetime.now()

    if file_name:
        # Process a specific file
        file_path = os.path.join(input_folder, file_name)
        if file_name.endswith('.csv'):
            try:
                combined_df = pd.read_csv(file_path, sep=',', header=None, on_bad_lines='skip', dtype=str)
                
                # Check the number of columns in the file
                num_columns = combined_df.shape[1]

                # Ensure that there are at least 26 columns
                if num_columns > 25:
                    # Filter rows where the value in the 26th column (index 25) is '1'
                    filtered_df = combined_df[combined_df[25] == '1']

                    # Save the filtered file to the output folder
                    output_file_path = os.path.join(output_folder, file_name)
                    filtered_df.to_csv(output_file_path, index=False)
                    
            except Exception as e:
                print(f"Error processing file {file_name}: {e}")
        else:
            print(f"File {file_name} is not a valid CSV file.")
    
    # End the timer and calculate time taken
    end_time = datetime.now()
    time_taken = end_time - start_time
    return time_taken.total_seconds()

def generate_dates(start_date, end_date):
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates

dates = generate_dates("20160209", "20241114")

def filter_countries(dates):
    total_time = 0
    file_count = 0

    for i in dates:
        # Process each file and accumulate processing time
        file_time = iterate_csv(f"{i}.csv")
        total_time += file_time
        file_count += 1

    # Calculate and print the average time per file processed
        if file_count > 0:
            average_time = total_time / file_count
            print(f"Processed {file_count} files.")
            print(f"Average time per file: {average_time:.2f} seconds.")
        else:
            print("No files processed.")

filter_countries(dates)
