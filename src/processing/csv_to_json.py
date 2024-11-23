import os
import csv
import json
import re
from googletrans import Translator
from tqdm import tqdm


def clean_text(text):
    """
    Cleans the text field by:
    - Decoding bytes if present.
    - Removing 'RT' when found by itself.
    - Removing mentions (words starting with '@').
    - Removing URLs.
    - Removing non-ASCII characters and special symbols.
    - Normalizing whitespace.
    - Translating to English if the source language is not English.
    """
    # Decode bytes if present
    if text.startswith("b'") or text.startswith('b"'):
        text = bytes(text[2:-1], "utf-8").decode("unicode_escape", errors="ignore")

    text = re.sub(r'\bRT\b', '', text)

    # Remove URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)

    # Add underscore between camel-case words or adjacent uppercase and lowercase letters
    text = re.sub(r'(?<=[a-z])(?=[A-Z])', '_', text)  # Lowercase followed by uppercase
    text = re.sub(r'(?<=[A-Z])(?=[A-Z][a-z])', '_', text)  # Uppercase followed by uppercase-lowercase
    text = re.sub(r'(?<=[A-Za-z])(?=\d)', '_', text)  # Letters followed by numbers
    text = re.sub(r'(?<=\d)(?=[A-Za-z])', '_', text)  # Numbers followed by letters

    # Remove non-ASCII characters
    text = re.sub(r"[^\x00-\x7F]+", "", text)

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text

def process_csv_to_json(input_folder, output_folder):
    """
    Processes all CSV files in the input folder and saves cleaned JSON files
    in the output folder, with a progress bar for each file.
    """
    os.makedirs(output_folder, exist_ok=True)

    # Get a list of all CSV files in the input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

    # Outer loop for files
    for filename in tqdm(csv_files, desc="Processing files", unit="file"):
        csv_file_path = os.path.join(input_folder, filename)
        data_list = []

        # Calculate total rows for the current file
        with open(csv_file_path, mode="r", encoding="utf-8") as file:
            total_rows = sum(1 for _ in file) - 1  # Exclude header
        with open(csv_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            # Progress bar for the current file
            with tqdm(total=total_rows, desc=f"Processing {filename}", unit="row", leave=False) as pbar:
                for row in reader:
                    tweet = clean_text(row.get("tweet", ""))
                    timestamp = row.get("date", "")
                    if tweet and timestamp:
                        data_list.append({"text": tweet, "timestamp": timestamp})
                    pbar.update(1)

        # Save JSON
        json_file_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.json")
        with open(json_file_path, mode="w", encoding="utf-8") as json_file:
            json.dump(data_list, json_file, ensure_ascii=False, indent=4)


if __name__ == "__main__":

    input_folder = "../data/twitter-celebrity-tweets-data/twitter-celebrity-tweets-data/"
    output_folder = "../json"

    process_csv_to_json(input_folder, output_folder)
    print(f"Processing complete. JSON files saved in: {output_folder}")