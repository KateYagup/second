from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def query5():
    print('hello!')
    spark_session = SparkSession.builder \
        .master("local") \
        .appName('IMDB analysis app') \
        .getOrCreate()

# Запрос 5
    df_akas = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.akas.tsv.gz", header=True)
    df_akas.show()
    df_basics = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.basics.tsv.gz", header=True)
    df_basics.show()

    sql5 = df_akas.join(df_basics, df_basics.tconst == df_akas.titleId).filter(f.col('isAdult') == 1 ). \
        groupBy('region').count().orderBy('count', ascending=False)
    sql5.show()
    sql5.write.csv('D:/dataPyton/sql5', header=True, mode='Overwrite')