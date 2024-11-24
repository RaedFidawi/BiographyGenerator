import os
import csv
import json
import re
from tqdm import tqdm
from mtranslate import translate
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count

# Cache to store previously translated tweets
translation_cache = {}

def clean_text(text):
    """
    Cleans the text field.
    """
    if text.startswith("b'") or text.startswith('b"'):
        text = bytes(text[2:-1], "utf-8").decode("unicode_escape", errors="ignore")

    text = re.sub(r'\bRT\b', '', text)
    text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
    text = re.sub(r'(?<=[a-z])(?=[A-Z])', '_', text)
    text = re.sub(r'(?<=[A-Z])(?=[A-Z][a-z])', '_', text)
    text = re.sub(r'(?<=[A-Za-z])(?=\d)', '_', text)
    text = re.sub(r'(?<=\d)(?=[A-Za-z])', '_', text)
    text = re.sub(r"[^\x00-\x7F]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def translate_text_offline(text):
    """
    Translates text to English using mtranslate (offline).
    Uses a cache to avoid redundant translations.
    """
    if text in translation_cache:
        return translation_cache[text]

    try:
        translated = translate(text, to_language="en")
        translation_cache[text] = translated  # Cache the result
        return translated
    except Exception as e:
        print(f"Error translating text: {text[:50]}... - {e}")
        return text  # Return original text if translation fails

def process_row(row):
    """
    Processes a single row: cleans, translates, and structures the data.
    """
    tweet = clean_text(row.get("tweet", ""))
    timestamp = row.get("date", "")
    if tweet and timestamp:
        translated_tweet = translate_text_offline(tweet)  # Translate tweet
        return {"text": translated_tweet, "timestamp": timestamp}
    return None

def process_csv_to_json(input_folder, output_folder):
    """
    Processes all CSV files in the input folder and saves cleaned JSON files
    in the output folder.
    """
    os.makedirs(output_folder, exist_ok=True)

    # Get all CSV files
    csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

    for filename in tqdm(csv_files, desc="Processing files", unit="file"):
        csv_file_path = os.path.join(input_folder, filename)
        json_file_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.json")

        with open(csv_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            rows = list(reader)

        # Use ThreadPoolExecutor for parallel processing
        data_list = []
        with ThreadPoolExecutor(max_workers=cpu_count() * 2) as executor:
            futures = [executor.submit(process_row, row) for row in rows]

            for future in tqdm(futures, desc=f"Processing {filename}", unit="row", leave=False):
                try:
                    result = future.result()
                    if result:
                        data_list.append(result)
                except Exception as e:
                    print(f"Error processing row: {e}")

        # Save JSON
        with open(json_file_path, mode="w", encoding="utf-8") as json_file:
            json.dump(data_list, json_file, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    input_folder = "../data/twitter-celebrity-tweets-data/twitter-celebrity-tweets-data/"
    output_folder = "../json"

    process_csv_to_json(input_folder, output_folder)
    print(f"Processing complete. JSON files saved in: {output_folder}")

