import os

# Folder containing the files
folder_path = r'Q:\Project_BOLT\Data_Storage\Stock_Data\Daily_Prices'

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if ' Historical Data' in filename:  # Check if the string exists in the file name
        old_path = os.path.join(folder_path, filename)
        
        # Create the new filename by removing ' Historical Data'
        new_filename = filename.replace(' Historical Data', '')
        new_path = os.path.join(folder_path, new_filename)
        
        # Rename the file
        os.rename(old_path, new_path)
        print(f"Renamed: {filename} to {new_filename}")

print("All files have been renamed!")
