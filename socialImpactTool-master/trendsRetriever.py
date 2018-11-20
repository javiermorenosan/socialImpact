import pytrends
from datetime import datetime, timedelta
from pytrends.request import TrendReq
import mysql.connector

class Trends:

    def __init__(self, word, ndays, searchId):
        self.word = word
        self.ndays = ndays
        self.searchId = searchId

    def requestTrends(self):
        searchId = self.searchId
        word = self.word
        substracted_days = self.ndays

        pytrend = TrendReq()

        now = datetime.now()
        past_date = now - timedelta(days=substracted_days)

        year_now = str(now.year)
        month_now = str(now.month)
        day_now = str(now.day)

        year_substracted = str(past_date.year)
        month_substracted = str(past_date.month)
        day_substracted = str(past_date.day)

        timeframe = year_substracted + "-" + month_substracted + "-" + day_substracted + " " + year_now + "-" + month_now + "-" + day_now

        kwlist = [word]

        pytrend.build_payload(kwlist, timeframe=timeframe)

        # Dictionary
        related_queries = pytrend.related_queries().items()
        # Pandas dataframe
       
        related_queries_info = related_queries[0][1]['top'].values.tolist()
        counter = 0
        
        for i in related_queries_info:
            if(counter<10):
                query = i[0].encode('utf-8')
                weight = int(i[1])
                cnx = mysql.connector.connect(user='x', password='x', host='x', database='x')
                cursor = cnx.cursor()
                add_query = ("INSERT INTO trendsRelatedQueries (searchId, query, weight) VALUES (%s, %s, %s)")
                data_query = (searchId, query, weight)
                cursor.execute(add_query, data_query)
                cnx.commit()
                cnx.close()
            counter+=1


        # Pandas dataframe
        interest = pytrend.interest_over_time()
        for i in list(interest.index):
            day = str(i).split(" ")[0]
            weight = int(interest.loc[i][0])
            
            cnx = mysql.connector.connect(user='x', password='x', host='x', database='x')
            cursor = cnx.cursor()
            add_query = ("INSERT INTO trendsInterest (searchId, date, weight) VALUES (%s, %s, %s)")
            data_query = (searchId, day, weight)
            cursor.execute(add_query, data_query)
            cnx.commit()
            cnx.close()
            