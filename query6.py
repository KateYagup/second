from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f


def query6():
    print('query6')
    spark_session = SparkSession.builder \
        .master("local") \
        .appName('IMDB analysis app') \
        .getOrCreate()

    # Запрос 6
    print('title.episode.tsv')
    df_episode = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.episode.tsv.gz", header=True)
    df_episode.show()

    print('title.basics.tsv')
    df_basics = spark_session.read.options(sep=r'\t').csv("d:/DataFilms/title.basics.tsv.gz", header=True)
    # df_basics.show()

    df_episode.filter(f.col('episodeNumber') != '\\N').groupby('episodeNumber').count().\
        orderBy('count', ascending=False).show(50)


    df_episode.join(df_basics, 'tconst').filter(f.col('episodeNumber') < 5) \
        .select('primaryTitle', 'episodeNumber').show() \

    sql6.write.csv('D:/dataPyton/sql6', header=True, mode='Overwrite')

        # .withColumn('title', f.split(df_basics['primaryTitle'], '\\.').getItem(0)) \
        # .orderBy('episodeNumber', ascending=False)
    # sql6.show()
    # Попробовать с originalNumber

    # sql6 = df_episode.join(df_basics, 'tconst').filter(f.col('episodeNumber') != '\\N') \
    #     .select('primaryTitle', 'episodeNumber') \
    #     .withColumn('title', f.split(df_basics['primaryTitle'], '\\.').getItem(0)) \
    #     .orderBy('episodeNumber', ascending=False)
    # sql6.show()
    # sql6.groupby('title').count().orderBy('title', ascending=True).show(100)

