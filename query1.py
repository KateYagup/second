from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def query1():
    print('hello!')
    spark_session = SparkSession.builder \
        .master("local") \
        .appName('IMDB analysis app') \
        .getOrCreate()

    #  зпрос 1
    df_akas = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.akas.tsv.gz", header=True)
    sql1 = df_akas.select('title', 'region').filter(f.col('region') == 'UA')
    sql1.show()
    sql1.limit(5)
    sql1.write.csv('D:/dataPyton/sql1', header=True, mode='Overwrite')
