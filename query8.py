from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f


def query8():
    print('query8!')
    spark_session = SparkSession.builder \
        .master("local") \
        .appName('IMDB analysis app') \
        .getOrCreate()

 # Запрос 8

    print('title.ratings.tsv')
    df_ratings = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.ratings.tsv.gz", header=True)
    # df_ratings.show()

    print('title.basics.tsv')
    df_basics = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.basics.tsv.gz", header=True)
    # df_basics.show()

    # Попытка 2
    window = Window.partitionBy('oneGenre').orderBy(f.col('oneGenre').desc(), f.col('averageRating').desc())

    df_rating = df_ratings.join(df_basics, 'tconst').select('originalTitle', 'genres', 'averageRating')\
        .withColumn('oneGenre', f.split('genres', ',').getItem(0))
    df_rating.show()
    sql8 = df_rating.withColumn('row_number', f.row_number().over(window))\
        .filter(f.col('row_number') <= 5).drop('row_number')
    sql8.show()

    sql8.write.csv('D:/dataPyton/sql8', header=True, mode='Overwrite')
    # Попытка  1
    # window = Window.orderBy('genres').partitionBy('genres')
    # sql8 = df_ratings.withColumn('averageRating_new', f.mean(f.col('averageRating')).over(window))\
    #                  .select('originalTitle', 'genres', 'averageRating', 'averageRating_new')

    # df_ratings.join(df_basics, 'tconst').select('originalTitle', 'genres', 'averageRating')
    # window = Window.orderBy('genres').partitionBy('genres')
    # sql8 = df_ratings.withColumn('averageRating_new', f.mean(f.col('averageRating')).over(window))\
    #                  .select('originalTitle', 'genres', 'averageRating', 'averageRating_new')
    # sql8.show()