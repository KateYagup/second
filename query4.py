from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def query4():
    print('query4!')
    spark_session = SparkSession.builder \
        .master("local") \
        .appName('IMDB analysis app') \
        .getOrCreate()

    # Запрос 4
    df = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/name.basics.tsv.gz", header=True)
    # df.show()
    df_basics = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.basics.tsv.gz", header=True)
    # df_basics.show()
    df_principals = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.principals.tsv.gz", header=True)\
        .limit(10000)

    # df_principals.show()
    print('sql4')
    sql4 = df_principals.join(df_basics, 'tconst').join(df, 'nconst')\
        .select('originalTitle', 'primaryName','characters')\
        .filter((f.col('originalTitle') == 'Carmencita') | (f.col('originalTitle') == 'Die Sünden der Väter'))
    sql4.show()
    sql4.write.csv('D:/dataPyton/sql4', header=True, mode='Overwrite')