import pandas as pd
import logging
import re
import csv
from autocorrect import Speller
from textblob import TextBlob

# Initialize Speller once to avoid recreating it in each function call
spell = Speller()

# Use Speller to correct typos in text
def correct_typos(text):
    corrected_text = spell(text) 
    return corrected_text

# Sentiment analysis using TextBlob
def analyze_sentiment(tweet):
    analysis = TextBlob(tweet)
    return analysis.sentiment.polarity

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Clean and standardize location
def clean_location(location):
    if not isinstance(location, str) or location.strip() == "":
        return "customized address"
    
    corrected_location = spell(location)
    
    # Standardize based on known location patterns (city/state or city/country)
    location_patterns = [
        r'\b[a-zA-Z]+, [a-zA-Z]{2,}\b',  # Matches city/country or city/state
        r'\b[a-zA-Z]+ [a-zA-Z]{2,}\b',   # Matches city followed by state or country (no comma)
    ]
    
    for pattern in location_patterns:
        if re.match(pattern, corrected_location):
            return corrected_location
    
    # If the location is not in a standard format, return "customized address"
    return "customized address"

# Clean tweet text
def clean_tweet(tweet):
    # Remove URLs, mentions, hashtags, and special characters
    tweet = re.sub(r"http\S+", "", tweet)  # Remove URLs
    tweet = re.sub(r"@\S+", "", tweet)  # Remove mentions
    tweet = re.sub(r"#\S+", "", tweet)  # Remove hashtags
    tweet = re.sub(r"[^A-Za-z0-9 ]+", "", tweet)  # Remove special characters
    tweet = tweet.lower()  # Convert to lowercase
    tweet = correct_typos(tweet)  # Correct typos using Speller
    return tweet

# Clean tweets from a CSV file and perform sentiment analysis
def clean_tweets(input_csv, output_csv):
    updated_data = []
    
    # Read comments from the input CSV file using ISO-8859-1 encoding
    with open(input_csv, mode='r', encoding='ISO-8859-1') as file:
        csv_reader = csv.DictReader(file)
        fieldnames = csv_reader.fieldnames  # Ensure the headers are maintained

        # Print column names to help diagnose the issue
        logging.info(f"CSV Column Names: {fieldnames}")

        # Handle common variations for the 'Tweet' column
        tweet_column = None
        for possible_name in ['Tweet', 'tweet', 'text', 'Text']:
            if possible_name in fieldnames:
                tweet_column = possible_name
                break

        if not tweet_column:
            raise ValueError("'Tweet' column is missing in the input CSV. Column names found: " + ", ".join(fieldnames))

        for row in csv_reader:
            # Process each row by cleaning and correcting the tweet text
            tweet = row.get(tweet_column, '')  # Retrieve tweet text from the correct column
            if tweet:  
                cleaned_tweet = clean_tweet(tweet)  # Clean the tweet text
                sentiment_score = analyze_sentiment(cleaned_tweet)  # Analyze sentiment
                row[tweet_column] = cleaned_tweet  # Overwrite the original 'Tweet' column
                row['SentimentScore'] = sentiment_score  # Add sentiment score
                row['Location'] = clean_location(tweet)
            # Clean 'hashtags' if present
            if 'hashtags' in row:
                row['hashtags'] = clean_tweet(row['hashtags'] or "")

            # Standardize the 'location' column
            if 'location' in row:
                row['location'] = clean_location(row['location'])

            # Append updated row to the list
            updated_data.append(row)

    # Write cleaned data to a new CSV file
    with open(output_csv, mode='w', newline='', encoding='utf-8') as file:
        # Ensure 'SentimentScore' is added to fieldnames
        csv_writer = csv.DictWriter(file, fieldnames=fieldnames + ['SentimentScore'])
        csv_writer.writeheader()
        csv_writer.writerows(updated_data)

    logging.info(f"Cleaned tweets have been saved to {output_csv}")

# Main function
if __name__ == "__main__":
    raw_tweet = "raw/tweets_data.csv"  # Adjust the input filename
    washed_tweet = '/processed/washed_tweets_with_sentiment.csv'  # Adjust the output filename
    clean_tweets(raw_tweet, washed_tweet)
