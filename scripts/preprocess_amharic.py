import pandas as pd
import os
from etnltk.common.preprocessing import (
    remove_whitespaces,
    remove_special_characters,
    remove_links,
    remove_tags,
    remove_emojis,
    remove_email,
    remove_digits,
    remove_english_chars,
    remove_arabic_chars,
    remove_chinese_chars
)

from etnltk.common.ethiopic import (
    remove_ethiopic_digits,
    remove_ethiopic_punctuation,
    remove_non_ethiopic
)

from etnltk.lang.am.preprocessing import (
    remove_punctuation,
    remove_stopwords
)

from etnltk.lang.am.normalizer import (
    normalize_char,
    normalize_labialized,
    normalize_punct,
    normalize_shortened
)

from etnltk.tokenize.am import sent_tokenize, word_tokenize


def preprocess_amharic_text(text):
    if not isinstance(text, str):
        print(f"Warning: Non-string input detected: {type(text)} -> converting to empty string")
        return "", [], []

    # Cleaning
    text = remove_whitespaces(text)
    text = remove_links(text)
    text = remove_email(text)
    text = remove_tags(text)
    text = remove_emojis(text)
    text = remove_special_characters(text)
    text = remove_digits(text)
    text = remove_english_chars(text)
    text = remove_arabic_chars(text)
    text = remove_chinese_chars(text)
    text = remove_ethiopic_digits(text)
    text = remove_ethiopic_punctuation(text)
    text = remove_non_ethiopic(text)
    text = remove_punctuation(text)
    
    text = remove_stopwords(text)
    if isinstance(text, list):
        text = " ".join(text)  # convert list back to string

    # Normalization
    text = normalize_char(text)
    text = normalize_labialized(text)
    text = normalize_shortened(text)
    text = normalize_punct(text)

    # Tokenization
    sentences = sent_tokenize(text)
    words = word_tokenize(text)

    return text, sentences, words


def preprocess_csv(input_csv_path, output_csv_path, text_column):
    df = pd.read_csv(input_csv_path)

    # Fill NaN and convert to string
    df[text_column] = df[text_column].fillna("").astype(str)

    df['cleaned_normalized_text'] = ""
    df['tokenized_sentences'] = ""
    df['tokenized_words'] = ""

    for idx, row in df.iterrows():
        original_text = row[text_column]  # now guaranteed string
        cleaned_text, sentences, words = preprocess_amharic_text(original_text)

        df.at[idx, 'cleaned_normalized_text'] = cleaned_text
        df.at[idx, 'tokenized_sentences'] = sentences
        df.at[idx, 'tokenized_words'] = words

    df.to_csv(output_csv_path, index=False)
    print(f"Preprocessed data saved to {output_csv_path}")

def save_processed_data(df, output_folder, filename):
    # Create the folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Drop the original message column
    if 'message' in df.columns:
        df = df.drop(columns=['message'])

    # Save to CSV
    output_path = os.path.join(output_folder, filename)
    df.to_csv(output_path, index=False)
    print(f"Processed data saved to {output_path}")


if __name__ == "__main__":
    input_csv = "../data/raw/telegram_messages_20250622_111837.csv"
    interim_output_csv = "../data/interim/tokenized_message.csv"
    processed_output_folder = "../data/processed"
    processed_output_file = "tokenized_message_processed.csv"
    text_col = "message"

    # Step 1: Preprocess and save interim version
    preprocess_csv(input_csv, interim_output_csv, text_col)

    # Step 2: Load interim, drop original message, save to processed folder
    df = pd.read_csv(interim_output_csv)
    save_processed_data(df, processed_output_folder, processed_output_file)

