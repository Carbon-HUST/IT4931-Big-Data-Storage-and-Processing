import requests
import pandas as pd
import time
from json import loads
from bs4 import BeautifulSoup
from datetime import timedelta, date

url_base = 'https://coinmarketcap.com/historical/'


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


start_date = date(2022, 11, 15)
end_date = date(2022, 11, 16)


def runaround(single_date):
    time.sleep(15)
    url = url_base + single_date.strftime("%Y%m%d")
    print(url)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    return soup.find('script', type='application/json')


for single_date in daterange(start_date, end_date):
    url = url_base + single_date.strftime("%Y%m%d")
    print(url)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    s = soup.find('script', type='application/json')
    while s is None:
        s = runaround(single_date)
    s = s.string
    start = s.find('[{\\"id\\":1')
    s = s[start:]
    end = s.find(',\\"page\\":1')
    s = s[:end]
    s = s.replace('\\', '')
    json = loads(s)
    df = pd.json_normalize(json)
    if df is not None:
        # print(df)
        df.to_csv('bigdata.csv', mode='w', header=True)
    else:
        print('no data:', single_date)
    time.sleep(3)


import matplotlib.pyplot as plt

from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("HDSD_Visual")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

data = spark.read.csv("bigdata.csv", sep=',', inferSchema=True, header=True)
data.show(20)
df = data.limit(20).toPandas()
df.head()


name = df['name'].head(20)
volume_24h = df['quote.USD.volume24h'].head(20)
percent_change_1h = df['quote.USD.percentChange1h'].head(20)

fig = plt.figure(figsize=(20, 20))
plt.bar(name[0:10], volume_24h[0:10])

# Show Plot
plt.show()
