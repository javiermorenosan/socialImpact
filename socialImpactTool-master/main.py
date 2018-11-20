from tweetsRetriever import Twitter
from newsRetriever import News
import os
import sys
from datetime import datetime, timedelta
import mysql.connector
import uuid
from trendsRetriever import Trends


word = sys.argv[1]

if(len(sys.argv) > 2):
	word = " ".join(sys.argv[1:len(sys.argv)])

ndays = 7

#twitter api params
twitter_requests_per_day = 20
tweets_per_request = 100
new_file_twitter = "/home/maria_dev/tempTweets.csv"

#news api params
news_request_per_day = 10
page_size = 100
new_file_news = "/home/maria_dev/tempNews.csv"

#We save the search in the database
now = datetime.now() 
year = str(now.year)
month = str(now.month)
day = str(now.day)
today = year+"-"+month+"-"+day

cnx = mysql.connector.connect(user='x', password='x', host='x', database='x')
cursor = cnx.cursor()

searchId = uuid.uuid4().int & (1<<31)-1

add_search = ("INSERT INTO searches (searchId, word, date) VALUES (%s, %s, %s)")
data_search = (searchId, word, today)

cursor.execute(add_search, data_search)
cnx.commit()
cnx.close()


twitter = Twitter(word = word, requests_per_day= twitter_requests_per_day, tweets_per_request= tweets_per_request, ndays=ndays, filename=new_file_twitter)
news = News(word = word, page_size= page_size, filename = new_file_news, ndays = ndays, requests_per_day= news_request_per_day)

print "Retrieving tweets..."
twitter.createCSV()

print "Sumbmitting Spark job..."
os.system('hadoop fs -copyFromLocal /home/maria_dev/tempTweets.csv /user/maria_dev')
os.system('spark-submit --master yarn --deploy-mode client --jars mysql-connector-java-8.0.13.jar tweetsProcessing.py '+str(searchId))

print "Retrieving news..."
news.createCSV()

print "Submitting Spark job..."
os.system('hadoop fs -copyFromLocal /home/maria_dev/tempNews.csv /user/maria_dev')
os.system('spark-submit --master yarn --deploy-mode client --jars mysql-connector-java-8.0.13.jar newsProcessing.py '+str(searchId))

os.system('rm /home/maria_dev/tempTweets.csv')
os.system('rm /home/maria_dev/tempNews.csv')
os.system('hadoop fs -rm  /user/maria_dev/tempTweets.csv')
os.system('hadoop fs -rm  /user/maria_dev/tempNews.csv')

print ("Retrieving trends...")
trends = Trends(word = word, ndays = ndays, searchId =searchId)
trends.requestTrends()
