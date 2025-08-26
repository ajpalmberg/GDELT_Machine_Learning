import openai
import os
import json
import csv

import sys
sys.path.append("Q:\\Project_BOLT")
from freq_used import OPEN_AI_KEY, gics_structure


sub_industries = []

def extract_sub_industries(data):
    for key, value in data.items():
        if "sub-industries" in value:
            sub_industries.extend(value["sub-industries"])
        else:
            extract_sub_industries(value)
            
extract_sub_industries(gics_structure)
print(len(sub_industries))

openai.api_key = OPEN_AI_KEY

def query_chatgpt_and_save_to_csv(company_name, sub_industries, filename):
    if os.path.exists(filename):
        print(f"File {filename} already exists. Skipping query.")
        return

    prompt = (
        f"Summarize the core business operations and industry focus of the company '{company_name}' in one concise sentence. "
        f"Then rate the relevance of the following sub-industries for the company '{company_name}' "
        f"on a scale of 0-100. A low score means 'not relevant at all', numbers near the middle range hold moderate relevance, "
        f"and a high score means 'highly relevant'. Use the full range of values, not always multiples of 10. "
        f"Provide the output in the following CSV format:\n\n"
        f"The first row should be the company summary.\n"
        f"The second row should be the headers: 'Sub-Industry', 'Relevance Score'.\n"
        f"The subsequent rows should list each sub-industry and its relevance score. Be STRICT and LOGICAL with scoring\n\n"
        f"Sub-Industries:\n"
    )
    prompt += "\n".join(f"- {sub}" for sub in sub_industries)

    try:
        response = openai.ChatCompletion.create(
            model="o1-mini",
            messages=[{"role": "user", "content": prompt}],
        )
        response_text = response['choices'][0]['message']['content']
        print("Raw response from GPT:\n", response_text)  
        response_lines = response_text.strip().split("\n")
        csv_rows = response_lines[3:-1]  

        with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for row in csv_rows:
                writer.writerow(row.split(","))

        print(f"Relevance scores saved to {filename}")
    except Exception as e:
        print(f"Error: {e}")


def csv_to_dict(file_path):
    result_dict = {}
    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) >= 2:
                    result_dict[row[0].strip()] = row[1].strip()
    except Exception as e:
        print(f"Error reading file: {e}")
    return result_dict

file_path = r"Q:\\Project_BOLT\\Data_Formation_Scripts\\Ticker_Classifications\\tickers.csv"
companies = csv_to_dict(file_path)

for i in companies:
    output_file = f"Q:\\Project_BOLT\\Data_Formation_Scripts\\Ticker_Classifications\\Ticker_csvs\\{i}.csv"
    query_chatgpt_and_save_to_csv(companies[i], sub_industries, output_file)