import csv
from newsapi import NewsApiClient
from datetime import datetime, timedelta
import math

class News:
    def __init__(self, word, page_size, filename, ndays, requests_per_day):
        self.word = word
        self.page_size = page_size
        self.filename = filename
        self.ndays = ndays
        self.requests_per_day = requests_per_day

    def createCSV(self):
        word = self.word


        page_size = self.page_size
        filename = self.filename
        ndays = self.ndays
        requests_per_day = self.requests_per_day

        newsapi = NewsApiClient(api_key='x')
        # /v2/top-headlines

        new_file = open(filename,
                        'wb')
        writer = csv.writer(new_file, delimiter='\t')

        now = datetime.now()
        year = str(now.year)
        month = str(now.month)
        day = now.day

        for i in range(0, ndays):
            page = 1
            from_date = now - timedelta(days=i + 1)

            from_day_str = str(from_date.day)

            if from_date.day < 10:
                from_day_str = "0"+from_day_str


            from_date_str = str(from_date.year) + "-" + str(from_date.month) + "-" + from_day_str

            #to do the first iteration
            total_results = page_size+1
            counterTitle = 0
            for j in range (0, requests_per_day):

                if (page <= math.ceil(total_results/page_size)):
                    articles = newsapi.get_everything(q=word, language='en', page_size=page_size, from_param=from_date_str, to=from_date_str, page=page)
                    total_results = articles["totalResults"]
                    page+=1
                    #print articles
                    for art in articles["articles"]:
                        news_list = []
                        
                        if(art["title"] and art["source"]["name"] and art["content"]):
                            news_list.append(word)
                            news_list.append(art["title"].encode('utf-8').replace("\n", ' ').replace("\r",' '))
                            news_list.append(from_date_str)
                            news_list.append(str(total_results))
                            news_list.append(art["source"]["name"])
                            news_list.append(art["content"].encode('utf-8').replace("\n", ' ').replace("\r",' '))
                            writer.writerow(news_list)

                       