from tqdm import tqdm
from googletrans import Translator
import os
import csv
import json
import re

translator = Translator()

def detect_file_language(file_path):
    """
    Detects the predominant language of the text in the file by sampling the first few rows.
    """
    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        sampled_texts = []
        for i, row in enumerate(reader):
            if i >= 5:  # Sample only the first 5 rows
                break
            tweet = row.get("tweet", "")
            if tweet:
                sampled_texts.append(tweet)

    combined_text = " ".join(sampled_texts)
    try:
        detected = translator.detect(combined_text)
        return detected.lang.split('-')[0]  # Return language code
    except Exception as e:
        print(f"Language detection error: {e}")
        return "en"  # Default to English if detection fails

def clean_text(text, src):
    """
    Cleans the text field by:
    - Decoding bytes if present.
    - Removing URLs.
    - Removing non-ASCII characters and special symbols.
    - Normalizing whitespace.
    - Translating to English if the source language is not English.
    """
    # Decode bytes if present
    if text.startswith("b'") or text.startswith('b"'):
        text = bytes(text[2:-1], "utf-8").decode("unicode_escape", errors="ignore")
    
    # Remove URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
    
    # Remove non-ASCII characters
    text = re.sub(r"[^\x00-\x7F]+", "", text)
    
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Translate text if not in English
    if src != "en":
        try:
            text = translator.translate(text, src=src, dest="en").text
        except Exception as e:
            print(f"Translation error: {e}")

    return text

def process_csv_to_json(input_folder, output_folder):
    """
    Processes all CSV files in the input folder and saves cleaned JSON files
    in the output folder, with a progress bar.
    """
    os.makedirs(output_folder, exist_ok=True)
    
    # Get a list of all CSV files in the input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

    # Initialize progress bar for files
    for filename in tqdm(csv_files, desc="Processing files", unit="file"):
        csv_file_path = os.path.join(input_folder, filename)
        data_list = []
        print(f"Detecting language for file: {filename}")
        
        # Detect file language
        src_language = detect_file_language(csv_file_path)
        print(f"Detected language: {src_language}")
        
        with open(csv_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            # Initialize progress bar for rows in the CSV file
            total_rows = sum(1 for _ in open(csv_file_path, encoding="utf-8")) - 1  # Exclude header
            file.seek(0)  # Reset file pointer to start
            for row in tqdm(reader, desc=f"Processing rows in {filename}", total=total_rows, leave=False):
                tweet = clean_text(row.get("tweet", ""), src=src_language)
                timestamp = row.get("date", "")
                if tweet and timestamp:
                    data_list.append({"text": tweet, "timestamp": timestamp})

        # Save JSON
        json_file_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.json")
        with open(json_file_path, mode="w", encoding="utf-8") as json_file:
            json.dump(data_list, json_file, ensure_ascii=False, indent=4)

if __name__ == "__main__":

    input_folder = "../data/twitter-celebrity-tweets-data/twitter-celebrity-tweets-data/"
    output_folder = "../json"

    process_csv_to_json(input_folder, output_folder)
    print(f"Processing complete. JSON files saved in: {output_folder}")