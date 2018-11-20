from pyspark import SparkContext
import re
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
import sys

searchId = int(sys.argv[1])
#searchId = 1

sc = SparkContext()
sqlContext = sql.SQLContext(sc)

stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through","like", "get", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "doesn", "didn", "shouldn", "wouldn", "couldn", "even", "just", "don", "should", "now", "could", "would"]

tweets = sc.textFile('/user/maria_dev/tempTweets.csv')

tweets_split = tweets.map(lambda l: l.split("\t"))

word_searched = tweets_split.map(lambda line: line[0]).take(1)[0].lower()

stopwords.append(word_searched)

tweets_types = tweets_split.map(lambda line: [int(line[1]), int(line[2]), int(line[3]), line[4], line[5]])

tweets_id_date_rt_fav = tweets_types.map(lambda line: [line[0], line[3], line[1], line[2]])
tweets_id_date_rt_fav = tweets_id_date_rt_fav.map(lambda line: [line[0], [line[3], line[1], line[2]]])

just_text = tweets_types.map(lambda line: [line[0], line[4]])

fields = [StructField('id', StringType(), True),StructField('text', StringType(), True)]
schema = StructType(fields)

#We assume that there is an rdd called just_text with lists containing the tweet id and text in each row
#just_text = [[id1, text1][id2, text2]....]
data_df = sqlContext.createDataFrame(just_text, schema)

tokenizer = Tokenizer(inputCol = "text", outputCol ="words")
tokenizedData = tokenizer.transform(data_df)
hashingTF = HashingTF(inputCol = "words", outputCol = "tf", numFeatures = 2**16)
tfData = hashingTF.transform(tokenizedData)

idf = IDF(inputCol = "tf", outputCol = "features")
idfModel = idf.fit(tfData)

finalData = idfModel.transform(tfData)

model = LogisticRegressionModel.load('/user/maria_dev/user/maria_dev/sentimentModel')

predictions = model.transform(finalData)

predictions2 = predictions.select(predictions.id, predictions.text, predictions.prediction)

#To create a regular rdd
predictions_rdd = predictions2.rdd.map(list)

predictions_without_text = predictions_rdd.map(lambda line: [int(line[0]), line[2]])

tweets_with_prediction = predictions_without_text.join(tweets_id_date_rt_fav)

tweets_with_prediction = tweets_with_prediction.map(lambda line: [line[0], line[1][0], line[1][1][2], line[1][1][0], line[1][1][1]])

date_prediction = tweets_with_prediction.map(lambda line: [line[4], line[1]])
predictions_groupBy_date_iterable = date_prediction.groupByKey()
predictions_groupBy_date = predictions_groupBy_date_iterable.map(lambda line: [line[0], list(line[1])])
#Percentage of positive/negative tweets per day:
pos_neg_per_day = predictions_groupBy_date.map(lambda line: [line[0], sum(line[1])/4.0/float(len(line[1]))*100, (1.0-sum(line[1])/4.0/float(len(line[1])))*100])
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print "1"
print("Per day results: ")
print(pos_neg_per_day.take(7))

positive_tweets = tweets_with_prediction.filter(lambda line: line[1] == 4.0)
negative_tweets = tweets_with_prediction.filter(lambda line: line[1] == 0)

retweets_p = positive_tweets.map(lambda line: [line[4], line[2]])
retweets_per_day_iterable_p = retweets_p.groupByKey()
retweets_per_day_p = retweets_per_day_iterable_p.map(lambda line: [line[0], list(line[1])])
avg_retweets_per_day_positive = retweets_per_day_p.map(lambda line: [line[0], float(sum(line[1]))/len(line[1])])

favs_p = positive_tweets.map(lambda line: [line[4], line[3]])
favs_per_day_iterable_p = favs_p.groupByKey()
favs_per_day_p = favs_per_day_iterable_p.map(lambda line: [line[0], list(line[1])])
avg_favs_per_day_positive = favs_per_day_p.map(lambda line: [line[0], float(sum(line[1]))/len(line[1])])

retweets_n = negative_tweets.map(lambda line: [line[4], line[2]])
retweets_per_day_iterable_n = retweets_n.groupByKey()
retweets_per_day_n = retweets_per_day_iterable_n.map(lambda line: [line[0], list(line[1])])
avg_retweets_per_day_negative = retweets_per_day_n.map(lambda line: [line[0], float(sum(line[1]))/len(line[1])])

favs_n = negative_tweets.map(lambda line: [line[4], line[3]])
favs_per_day_iterable_n = favs_n.groupByKey()
favs_per_day_n = favs_per_day_iterable_n.map(lambda line: [line[0], list(line[1])])
avg_favs_per_day_negative = favs_per_day_n.map(lambda line: [line[0], float(sum(line[1]))/len(line[1])])

print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"
print "2"

print avg_retweets_per_day_positive.take(10)
print avg_retweets_per_day_negative.take(10)
print avg_favs_per_day_positive.take(10)
print avg_favs_per_day_negative.take(10)

tweets_texts = tweets_types.map(lambda line: [line[3], line[4]])

#We lowercase all the content
text_lower = tweets_texts.map(lambda line: [line[0], line[1].lower()])
#We remove numbers and punctuantion
words = text_lower.map(lambda line: [line[0], re.findall(r'(?<![@])\b\w+\b', line[1])])

words_byKey = words.flatMap(lambda line: [(line[0], w) for w in line[1]])

#We remove stopwords and those words with len < 2 
words_byKey_clean = words_byKey.filter(lambda line: len(line[1])>3 and (line[1] not in stopwords))

words_clean_iterable = words_byKey_clean.groupByKey()

#words_clean = words_clean_iterable.map(lambda line: [line[0], list(line[1])])
#words_clean = words_clean_iterable.map(lambda line: [line[0], line[1]])
words_clean = words_clean_iterable.map(lambda line: [list(line[1])])

words_joined = words_clean.map(lambda line: " ".join(line[0]))
words_total = words_joined.flatMap(lambda line: line.split(" "))
words_byKey_2 = words_total.map(lambda line: (line, 1))

count_words = words_byKey_2.reduceByKey(lambda x, y: x + y)
words_count_first = count_words.map(lambda line: [line[1], line[0]])
words_ordered = words_count_first.sortByKey(False)
reference = float(words_ordered.map(lambda line: line[0]).take(1)[0])
words_ordered_percentage = words_ordered.map(lambda line: [float(line[0])/reference*100.0, line[1]])

print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print "3"
print words_ordered_percentage.take(15)

#twitterDaily table
fields2 = [StructField('searchId', StringType(), True), StructField('rtPos', DoubleType(), True), StructField('rtNeg', DoubleType(), True),  StructField('favPos', DoubleType(), True), StructField('favNeg', DoubleType(), True), StructField('tweetsPosPercentage', DoubleType(), True), StructField('tweetsNegPercentage', DoubleType(), True), StructField('date', StringType(), True)]
schema2 = StructType(fields2)

neg_per_day = pos_neg_per_day.map(lambda line: [line[0], line[2]])
twitterDailyData = avg_retweets_per_day_positive.join(avg_retweets_per_day_negative).join(avg_favs_per_day_positive).join(avg_favs_per_day_negative).join(pos_neg_per_day).join(neg_per_day)
twitterDaily = twitterDailyData.map(lambda line: [searchId, line[1][0][0][0][0][0], line[1][0][0][0][0][1], line[1][0][0][0][1], line[1][0][0][1], line[1][0][1], line[1][1], line[0]])

data_df2 = sqlContext.createDataFrame(twitterDaily, schema2)
data_df2.write.format('jdbc').options(url = 'jdbc:mysql://XXX.XXX.XXX.XXX:XXXX/socialImpact', driver='com.mysql.jdbc.Driver', dbtable='twitterDaily', user='X', password='X').mode('append').save()

#twitterFrequentWords table
fields3 = [StructField('searchId', StringType(), True), StructField('word', StringType(), True), StructField('weight', DoubleType(), True)]
schema3 = StructType(fields3)

twitterFrequentWordsList = words_ordered_percentage.map(lambda line: [searchId, line[1], line[0]]).take(10)
twitterFrequentWords = sc.parallelize(twitterFrequentWordsList)

data_df3 = sqlContext.createDataFrame(twitterFrequentWords, schema3)
data_df3.write.format('jdbc').options(url = 'jdbc:mysql://XXX.XXX.XXX.XXX:XXXX/socialImpact', driver='com.mysql.jdbc.Driver', dbtable='twitterFrequentWords', user='X', password='X').mode('append').save()



