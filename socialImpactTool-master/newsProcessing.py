from pyspark import SparkContext
import re
import sys
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel

searchId = int(sys.argv[1])


sc = SparkContext()
sqlContext = sql.SQLContext(sc)

stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "chars", "into", "through","like", "get", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "doesn", "didn", "shouldn", "wouldn", "couldn", "even", "just", "don", "should", "now", "could", "would"]

news = sc.textFile('/user/maria_dev/tempNews.csv')

news_split = news.map(lambda l: l.split("\t"))

word_searched = news_split.map(lambda line: line[0]).take(1)[0].lower()

stopwords.append(word_searched)

news_types = news_split.map(lambda line: [line[1], line[2], int(line[3]), line[4], line[5]])

news_title_date = news_types.map(lambda line: [line[0], line[1]])

news_title_source = news_types.map(lambda line: [line[0], line[3]])

just_text = news_types.map(lambda line: [line[0], line[4]])

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

predictions_without_text = predictions_rdd.map(lambda line: [line[0], line[2]])

news_with_prediction = predictions_without_text.join(news_title_date)

news_with_prediction = news_with_prediction.map(lambda line: [line[0], line[1][0], line[1][1]])

news_sources_prediction = news_with_prediction.join(news_title_source)

news_sources_prediction_iterable = news_sources_prediction.map(lambda line: [line[1][1], line[1][0]]).groupByKey()
news_sources_prediction = news_sources_prediction_iterable.map(lambda line: [line[0], list(line[1])])

negative_sources_1 = news_sources_prediction.map(lambda line: [[x for x in line[1] if x == 0.0], line[0]])
negative_sources = sc.parallelize(negative_sources_1.map(lambda line: [len(line[0]), line[1]]).sortByKey(False).take(5))

positive_sources = sc.parallelize(news_sources_prediction.map(lambda line: [float(sum(line[1])), line[0]]).sortByKey(False).take(5))
print "A"
print "A"
print "A"
print "A"
print "A"
print "A"
print "A"
print "A"
print "A"
print "A"
print "A"
print positive_sources.take(5)
print negative_sources.take(5)

news_with_prediction_date_first = news_with_prediction.map(lambda line: [line[2], line[1]])

news_with_prediction_per_day_iterable = news_with_prediction_date_first.groupByKey()
news_with_prediction_per_day = news_with_prediction_per_day_iterable.map(lambda line: [line[0], list(line[1])])

#Percentage of positive/negative news per day:
pos_neg_per_day = news_with_prediction_per_day.map(lambda line: [line[0], sum(line[1])/4.0/float(len(line[1]))*100, (1.0-sum(line[1])/4.0/float(len(line[1])))*100])
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

#Total results (7 days):
number_of_news = news_with_prediction.count()
number_of_positive_news = float(news_with_prediction.filter(lambda line: line[1] == 4.0).count())/float(number_of_news)*100.0
number_of_negative_news = float(news_with_prediction.filter(lambda line: line[1] == 0).count())/float(number_of_news)*100.0
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
print("Total results")
print("Positive news " + str(number_of_positive_news) + " %")
print("Negative news " + str(number_of_negative_news) + " %")

# TITLES
news_titles = news_types.map(lambda line: [line[1], line[0]])

#We lowercase all the content
text_lower_t = news_titles.map(lambda line: [line[0], line[1].lower()])
#We remove numbers and punctuantion
words_t = text_lower_t.map(lambda line: [line[0], re.split(r'\W+', line[1])])

words_byKey_t = words_t.flatMap(lambda line: [(line[0], w) for w in line[1]])

#We remove stopwords and those words with len < 2 
words_byKey_clean_t = words_byKey_t.filter(lambda line: len(line[1])>2 and (line[1] not in stopwords))

words_clean_iterable_t = words_byKey_clean_t.groupByKey()

words_clean_t = words_clean_iterable_t.map(lambda line: [list(line[1])])

words_joined_t = words_clean_t.map(lambda line: " ".join(line[0]))
words_total_t = words_joined_t.flatMap(lambda line: line.split(" "))
words_byKey_2_t = words_total_t.map(lambda line: (line, 1))

count_words_t = words_byKey_2_t.reduceByKey(lambda x, y: x + y)
words_count_first_t = count_words_t.map(lambda line: [line[1], line[0]])

#Most related words in titles:
words_ordered_title = words_count_first_t.sortByKey(False)
reference = float(words_ordered_title.map(lambda line: line[0]).take(1)[0])
words_ordered_title_percentage = words_ordered_title.map(lambda line: [float(line[0])/reference*100.0, line[1]])
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
print words_ordered_title_percentage.take(15)

# CONTENT
news_content = news_types.map(lambda line: [line[1], line[4]])

#We lowercase all the content
text_lower_c = news_content.map(lambda line: [line[0], line[1].lower()])
#We remove numbers and punctuantion
words_c = text_lower_c.map(lambda line: [line[0], re.split(r'\W+', line[1])])

words_byKey_c = words_c.flatMap(lambda line: [(line[0], w) for w in line[1]])

#We remove stopwords and those words with len < 2 
words_byKey_clean_c = words_byKey_c.filter(lambda line: len(line[1])>2 and (line[1] not in stopwords))

words_clean_iterable_c = words_byKey_clean_c.groupByKey()

words_clean_c = words_clean_iterable_c.map(lambda line: [list(line[1])])

words_joined_c = words_clean_c.map(lambda line: " ".join(line[0]))
words_total_c = words_joined_c.flatMap(lambda line: line.split(" "))
words_byKey_2_c = words_total_c.map(lambda line: (line, 1))

count_words_c = words_byKey_2_c.reduceByKey(lambda x, y: x + y)
words_count_first_c = count_words_c.map(lambda line: [line[1], line[0]])

#Most related words in contents:
words_ordered_content = words_count_first_c.sortByKey(False)
reference2 = float(words_ordered_content.map(lambda line: line[0]).take(1)[0])
words_ordered_content_percentage = words_ordered_content.map(lambda line: [float(line[0])/reference2*100.0, line[1]])

print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print "4"
print words_ordered_content_percentage.take(15)

#Number of news per day
news_per_day_iterable = news_split.map(lambda line: [line[2], line[3]]).groupByKey()
news_per_day = news_per_day_iterable.map(lambda line: [line[0], list(line[1])[0]])

#MySQL

#positive and negative news
pos_neg_per_day_searchId = pos_neg_per_day.map(lambda line: [searchId,line[1], line[2], line[0]])
fields = [StructField('searchId', IntegerType(), True),StructField('newsPosPercentage', DoubleType(), True), StructField('newsNegPercentage', DoubleType(), True), StructField('date', StringType(), True)]
schema = StructType(fields)
newsPosNegDf = sqlContext.createDataFrame(pos_neg_per_day_searchId , schema)
newsPosNegDf.write.format('jdbc').options(url = 'jdbc:mysql://XXX.XXX.XXX.XXX:XXXX/socialImpact', driver='com.mysql.jdbc.Driver', dbtable='newsDaily', user='X', password='X').mode('append').save()

#words in content
words_content_searchId = words_ordered_content_percentage.map(lambda line: [searchId, line[1], line[0]]).take(10)
words_content_searchId_rdd = sc.parallelize(words_content_searchId)
fields = [StructField('searchId', IntegerType(), True),StructField('word', StringType(), True), StructField('weight', DoubleType(), True)]
schema = StructType(fields)
words_content_searchId_df = sqlContext.createDataFrame(words_content_searchId_rdd , schema)
words_content_searchId_df.write.format('jdbc').options(url = 'jdbc:mysql://XXX.XXX.XXX.XXX:XXXX/socialImpact', driver='com.mysql.jdbc.Driver', dbtable='wordsContents', user='X', password='X').mode('append').save()

#words in title
words_title_searchId = words_ordered_title_percentage.map(lambda line: [searchId, line[1], line[0]]).take(10)
words_title_searchId_rdd = sc.parallelize(words_title_searchId)
fields = [StructField('searchId', IntegerType(), True),StructField('word', StringType(), True), StructField('weight', DoubleType(), True)]
schema = StructType(fields)
words_title_searchId_df = sqlContext.createDataFrame(words_title_searchId_rdd , schema)
words_title_searchId_df.write.format('jdbc').options(url = 'jdbc:mysql://XXX.XXX.XXX.XXX:XXXX/socialImpact', driver='com.mysql.jdbc.Driver', dbtable='wordsTitles', user='X', password='X').mode('append').save()

#positive sources
positive_sources_rdd = positive_sources.map(lambda line: [searchId, line[1]])
fields = [StructField('searchId', IntegerType(), True), StructField('sources', StringType(), True)]
schema = StructType(fields)
positive_sources_df = sqlContext.createDataFrame(positive_sources_rdd , schema)
positive_sources_df.write.format('jdbc').options(url = 'jdbc:mysql://XXX.XXX.XXX.XXX:XXXX/socialImpact', driver='com.mysql.jdbc.Driver', dbtable='newsPosSources', user='X', password='X').mode('append').save()

#negative sources
negative_sources_rdd = negative_sources.map(lambda line: [searchId, line[1]])
fields = [StructField('searchId', IntegerType(), True), StructField('sources', StringType(), True)]
schema = StructType(fields)
negative_sources_df = sqlContext.createDataFrame(negative_sources_rdd , schema)
negative_sources_df.write.format('jdbc').options(url = 'jdbc:mysql://XXX.XXX.XXX.XXX:XXXX/socialImpact', driver='com.mysql.jdbc.Driver', dbtable='newsNegSources', user='X', password='X').mode('append').save()

#News per day
news_per_day_rdd = news_per_day.map(lambda line: [searchId, int(line[1]), line[0]])
fields = [StructField('searchId', IntegerType(), True), StructField('results', IntegerType(), True), StructField('date', StringType(), True)]
schema = StructType(fields)
news_per_day_df = sqlContext.createDataFrame(news_per_day_rdd , schema)
news_per_day_df.write.format('jdbc').options(url = 'jdbc:mysql://XXX.XXX.XXX.XXX:XXXX/socialImpact', driver='com.mysql.jdbc.Driver', dbtable='newsResultsPerDay', user='X', password='X').mode('append').save()
