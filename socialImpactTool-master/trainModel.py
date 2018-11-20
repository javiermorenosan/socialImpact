from pyspark import SparkContext
import re
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel

sc = SparkContext()

stopwords = ['ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than']

tweets = sc.textFile('/user/maria_dev/tweetsDataSet.csv')
#tweets_split = sc.parallelize(tweets.map(lambda line: line.split('",')).takeSample(False,100000))
tweets_split = tweets.map(lambda line: line.split('",'))
#We select the tweet id and the content
tweets_text = tweets_split.map(lambda line: [line[1], line[5]])
#We lowercase all the content
text_lower = tweets_text.map(lambda line: [line[0], line[1].lower()])
#We remove numbers and punctuantion
words = text_lower.map(lambda line: [line[0], re.split(r'\W+', line[1])])

words_byKey = words.flatMap(lambda line: [(line[0], w) for w in line[1]])

#We remove stopwords and those words with len < 2 
words_byKey_clean = words_byKey.filter(lambda line: len(line[1])>2 and (line[1] not in stopwords))

words_clean_iterable = words_byKey_clean.groupByKey()
words_clean = words_clean_iterable.map(lambda line: [line[0], list(line[1])])
text = words_clean.map(lambda line: [line[0], " ".join(line[1])])
labels = tweets_split.map(lambda line: [line[1], line[0]])
labels_noQuotationMarks = labels.map(lambda line: [line[0], int(line[1].replace('"',''))])

text_labels = text.join(labels_noQuotationMarks)
text_labels_noId = text_labels.map(lambda line: line[1])

fields = [StructField('text', StringType(), True), StructField('label', IntegerType(), True)]

schema = StructType(fields)
data_df = spark.createDataFrame(text_labels_noId, schema)

tokenizer = Tokenizer(inputCol = "text", outputCol ="words")
tokenizedData = tokenizer.transform(data_df)
hashingTF = HashingTF(inputCol = "words", outputCol = "tf", numFeatures = 2**16)
tfData = hashingTF.transform(tokenizedData)

idf = IDF(inputCol = "tf", outputCol = "features")
idfModel = idf.fit(tfData)

finalData = idfModel.transform(tfData)

finalData.show(5)
finalData_labels_features = finalData.select('label','features')
(train, test) = finalData_labels_features.randomSplit([0.9, 0.1])

lr = LogisticRegression(maxIter=50)
model = lr.fit(train)
predictions = model.transform(test)

accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(test.count())
print accuracy
#0.76
TPR = float(predictions.filter((predictions.prediction == 4) & (predictions.label == 4)).count())/float((predictions.filter(predictions.label == 4).count()))
print TPR
#0.77
TNR = float(predictions.filter((predictions.prediction == 0) & (predictions.label == 0)).count())/float((predictions.filter(predictions.label == 0).count()))
print TNR
#0.75

model.save('/user/maria_dev/sentimentModel')
#m2 = LogisticRegressionModel.load('sentimentModel')
