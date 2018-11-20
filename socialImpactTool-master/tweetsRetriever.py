import tweepy
from datetime import datetime, timedelta
import csv

class Twitter:

    def __init__(self, word, requests_per_day, tweets_per_request, ndays, filename):
        self.word = word
        self.requests_per_day = requests_per_day
        self.tweets_per_request = tweets_per_request
        self.ndays = ndays
        self.filename = filename

    def createCSV (self):
        word = self.word
        word = "%20".join(word.split(" "))

        requests_per_day = self.requests_per_day
        tweets_per_request = self.tweets_per_request
        ndays = self.ndays
        filename = self.filename

        auth = tweepy.OAuthHandler("x", "x")
        auth.set_access_token("x", "x")

        api = tweepy.API(auth, wait_on_rate_limit=True)

        now = datetime.now()

        new_file = open(filename, 'wb')

        writer = csv.writer(new_file, delimiter='\t')

        for i in range(0, ndays):
            from_date = now - timedelta(days=i + 1)
            to_date = now - timedelta(i)

            from_date_str = str(from_date.year) + "-" + str(from_date.month) + "-" + str(from_date.day)
            to_date_str = str(to_date.year) + "-" + str(to_date.month) + "-" + str(to_date.day)

            # queries tweets published on from_day
            query1 = word + " since:" + from_date_str + " until:" + to_date_str + " lang:en"
            # If we want to retrieve tweets with no urls
            query1 = query1+" -filter:links"

            # If we want to filter retweets
            query1 = query1 + " -filter:retweets"

            id = 0
            for j in range(0, requests_per_day):
                if (id != 0):
                    # If it is not the first request, we set the maximum id (to avoid retreiving always the same tweets)
                    query = query1 + " max_id:" + str(id)
                else:
                    query = query1

                tweets = api.search(q=query, count=tweets_per_request, tweet_mode='extended')
                for tweet in tweets:
                    #print query

                    id = tweet.id

                    #print tweet.retweet_count
                    #print tweet.created_at
                    #print tweet.full_text.encode('utf-8').replace("\n", "")
                    tweet_list = [word, tweet.id, tweet.favorite_count, tweet.retweet_count, from_date_str, tweet.full_text.encode('utf-8').replace("\n", "")]

                    writer.writerow(tweet_list)

