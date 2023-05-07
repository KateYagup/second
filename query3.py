from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def query3():
    print('query3!')
    spark_session = SparkSession.builder \
        .master("local") \
        .appName('IMDB analysis app') \
        .getOrCreate()

    # Запрос 3
    df_basics = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.basics.tsv.gz", header=True)
    # df_basics.show()
    sql3 = df_basics.select('originalTitle', 'runtimeMinutes').filter(f.col('runtimeMinutes') > 2)
    sql3.show()
    sql3.write.csv('D:/dataPyton/sql3', header=True, mode='Overwrite')