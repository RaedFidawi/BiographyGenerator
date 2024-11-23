import asyncio
from twikit import Client
import numpy
import pandas as pd


credentials = open("account.txt", "r").readlines()

USERNAME = credentials[0].strip()
EMAIL = credentials[1].strip()
PASSWORD = credentials[2].strip()

DATA_FILE = "data.csv"

df = pd.read_csv(DATA_FILE)
df = df.drop(labels=["twitter_username", "domain", "followers_count"], axis=1)

user_ids = df["twitter_userid"].tolist()
names    = df["name"].tolist()
tweet_counts = df["tweet_count"].tolist() 

for entry in user_ids:
    print(entry)
    break

# Initialize client
client = Client('en-US', user_agent="Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0")

async def main():
    await client.login(
        auth_info_1=USERNAME ,
        auth_info_2=EMAIL,
        password=PASSWORD
    )
    
    # tweets = await client.get_user_tweets('27260086', 'Tweets')

    # tweet1 = tweets[0]
    # print(tweet1.text)
    # for tweet in tweets:
    #     print(tweet.text)
    

asyncio.run(main())