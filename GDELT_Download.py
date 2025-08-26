from datetime import datetime, timedelta
import requests
import os
import zipfile

def generate_dates(start_date, end_date):
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates

def populate_GDELT_data():

    dates = generate_dates("20130401", "20241114")

    output_directory = r"Q:\Project_BOLT\Data_Storage\GDELT_News"

    os.makedirs(output_directory, exist_ok=True)

    for i in dates:
        try:
            # Downloading the .zip file
            zip_file_path = os.path.join(output_directory, f"{i}.export.CSV.zip")
            response = requests.get(f"http://data.gdeltproject.org/events/{i}.export.CSV.zip")
            response.raise_for_status()

            with open(zip_file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            print(f"Downloaded {zip_file_path}")

            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:

                zip_ref.extractall(output_directory)
                print(f"Extracted {zip_file_path} to {output_directory}")

                extracted_files = zip_ref.namelist()
                if extracted_files:
                    extracted_file_path = os.path.join(output_directory, extracted_files[0])

                    renamed_file_path = os.path.join(output_directory, f"{i}.CSV")
                    os.rename(extracted_file_path, renamed_file_path)
                    print(f"Renamed extracted file to {renamed_file_path}")

            os.remove(zip_file_path)
            print(f"Removed {zip_file_path} after extraction and renaming")

        except requests.exceptions.RequestException as e:
            print(f"Issue with download for {i}: {e}")
        except zipfile.BadZipFile as e:
            print(f"Issue extracting {zip_file_path}: {e}")

# Call the function
populate_GDELT_data()
