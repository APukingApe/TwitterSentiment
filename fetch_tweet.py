import os
import tweepy
import pandas as pd
import logging
import time
from dotenv import load_dotenv


# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET'
                                
auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# Fetch tweets based on region (without keywords)
def fetch_tweets_by_region(geocode, max_tweets=1000, lang='en'):
    tweets_data = []
    tweet_count = 0
    max_id = None  

    logging.info(f"Starting tweet fetch for region: {geocode}")

    while tweet_count < max_tweets:
        try:
            # Fetch tweets using Cursor to handle pagination
            tweets = tweepy.Cursor(api.search_tweets, 
                                   geocode=geocode, 
                                   lang=lang, 
                                   tweet_mode='extended', 
                                   max_id=max_id).items(min(100, max_tweets - tweet_count))

            for tweet in tweets:
                tweet_data = {
                    'Created_At': tweet.created_at,
                    'User': tweet.user.screen_name,
                    'Tweet': tweet.full_text,
                    'Location': tweet.user.location,
                    'Coordinates': tweet.coordinates,
                    'Retweet_Count': tweet.retweet_count,
                    'Favorite_Count': tweet.favorite_count
                }
                tweets_data.append(tweet_data)
                tweet_count += 1
                max_id = tweet.id  

                if tweet_count >= max_tweets:
                    break

            logging.info(f"Fetched {tweet_count} tweets so far.")

        except tweepy.TweepError as e:
            logging.error(f"Error encountered: {str(e)}. Retrying in 15 minutes.")
            time.sleep(15 * 60)  

    logging.info(f"Tweet fetch completed. Total tweets fetched: {tweet_count}")
    return tweets_data

# Save tweets to a CSV file
def save_to_csv(tweets_data, filename="tweets_data.csv"):
    df = pd.DataFrame(tweets_data)
    df.to_csv(filename, index=False)
    logging.info(f"Tweets data saved to {filename}")

# Main function
if __name__ == "__main__":
    geocode = "37.7749,-122.4194,50km"  # Replace with  target region in the format of: latitude,longitude,radius)
    max_tweets = 500  

    tweets_data = fetch_tweets_by_region(geocode, max_tweets)
    save_to_csv(tweets_data, filename="raw_tweets_by_region.csv")
