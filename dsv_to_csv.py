import os
import csv

# Folder containing the files
input_folder = 'Q:\Project_BOLT\Data_Storage\Stock_Data\Options_Data_Text'
output_folder = 'Q:\Project_BOLT\Data_Storage\Stock_Data\Options_Data_CSV'

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

# Loop through all files in the folder
for filename in os.listdir(input_folder):
    if filename.endswith('.txt'):  # Process only .txt files
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.csv")
        
        # Convert each file
        with open(input_path, 'r') as infile, open(output_path, 'w', newline='') as outfile:
            reader = csv.reader(infile, delimiter='\t')  # Read with tab delimiter
            writer = csv.writer(outfile)  # Write as CSV
            
            for row in reader:
                writer.writerow(row)
        
        print(f"Converted: {filename} to {output_path}")

print("All files have been converted!")
