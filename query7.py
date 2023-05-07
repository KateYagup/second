from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f


def query7():
    print('query7!')
    spark_session = SparkSession.builder \
        .master("local") \
        .appName('IMDB analysis app') \
        .getOrCreate()

    # Запрос 7
    print('title.ratings.tsv')
    df_ratings = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.ratings.tsv.gz", header=True)
    # df_ratings.show()

    print('title.basics.tsv')
    df_basics = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.basics.tsv.gz", header=True)
    # df_basics.show()

    window = Window.partitionBy('decades').orderBy(f.col('averageRating').desc(), f.col('decades').desc())

    sql71 = df_ratings.join(df_basics, 'tconst').select('originalTitle', 'averageRating', 'numVotes', 'startYear') \
        .withColumn('decades', f.floor(df_basics.startYear / 10)).filter(f.col('startYear') != '\\N')
    # sql7.show()
    sql7 = sql71.withColumn('row_number', f.row_number().over(window)) \
        .filter(f.col('row_number') < 10)
    sql7.show(100)

    sql7.write.csv('D:/dataPyton/sql7', header=True, mode='Overwrite')

    # Первая попытка

    # window = Window.partitionBy('decades')\
    #     .orderBy(f.col('averageRating').desc(), f.col('rcount').desc()).
    #
    # j_table = df_ratings.join(df_basics, 'tconst').select('originalTitle', 'averageRating', 'numVotes', 'startYear') \
    #     .withColumn('decades', f.floor(df_basics.startYear / 10)).orderBy('startYear', ascending=False) \
    #     .filter(f.col('rcount') < 10)
    #     # .filter(f.col('startYear') != '\\N').limit(10)
    # sql7 = j_table.withColumn('popular', f.mean(f.col('averageRating')).over(window))
    # sql7.show()

    # df_ratings.join(df_basics, 'tconst').select('originalTitle', 'averageRating', 'numVotes', 'startYear') \
    #     .withColumn('decades', f.floor(df_basics.startYear / 10)).orderBy('startYear', ascending=False) \
    #     .filter(f.col('startYear') != '\\N').show()


