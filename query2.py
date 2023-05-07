from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def query2():
    print('query2!')
    spark_session = SparkSession.builder \
        .master("local") \
        .appName('IMDB analysis app') \
        .getOrCreate()

    # Запрос 2
    df = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/name.basics.tsv.gz", header=True)
    sql2 = df.select('primaryName', 'birthYear').filter(f.col('birthYear') < 1900)
    sss = df.select('primaryName', 'birthYear')
    sql2.show()
    sql2.write.csv('D:/dataPyton/sql2', header=True, mode='Overwrite')
