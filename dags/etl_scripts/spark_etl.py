"""
spark_etl_job.py
~~~~~~~~~~
This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'

"""

from pyspark.sql.functions import from_unixtime, col, to_timestamp,explode,regexp_replace,translate,count,countDistinct,month,max
from pyspark.sql.window import Window
import argparse
from spark_util import start_spark
from generic_functions import remove_file
import gc
import os

def main(input_file):
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(app_name='data_import')

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    spark_etl = DataLoad(input_file,spark)
    spark_etl.load_data()
    spark_etl.transform_data()
    spark_etl.unload_data()
    del spark_etl
    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None

class DataLoad:

    """ Intialization expecting input file path and filename ,all other variables are
        optional.If nothing is provided by default file delim will be ',' and db will be test
        and schema will be public
    """
    def __init__(self,input_file,spark):
        self.input_file=input_file
        self.spark=spark

    def load_data(self):
        """Load data from json file format.
        Assignment 1 the data will be loaded after calling the below method
        S3 file can be downloaded by any of the method like wget/boto3 etc
        I kept the file and test file as part of Docker image
        """
        self.df = self.spark.read.json(self.input_file)
        ## 1. JSON schema
        print('********Query_1********')
        self.df.printSchema()
        print("Raw Data Count: " + str(self.df.count()))
        print('********Query_1********')

    def transform_data(self):
        """Transform original dataset.
           Creating Temp table since writing logic in SQL is more readable
           SQL is portable with Scala and Python versions.
           Same can be done using normal DF filters and case statement
        """

        ##2 DQc and cleansed Date

        print('********Query_2:Cleansed DF count after DQC********')

        df_dqc = self.df.filter("(title is not null or length(title)>0) and number_of_pages>20 and publish_date between 1950 and current_date")
        df_dqc.repartition(3).count()
        print("Cleansed DF Count: " + str(df_dqc.repartition(3).count()))
        print('********Cleansed DF ********')

        self.df_author_explode = (df_dqc
                             .withColumn('created_time', to_timestamp("created.value"))
                             .filter("created_time is not null")
                             .select(explode("authors.key").alias("author"), "created_time", "publish_date", "title","number_of_pages")
                             .filter("author is  NOT NULL")
                             .select(regexp_replace('author', '/authors/', '').alias("author"), "created_time",
                                     "publish_date", "title","number_of_pages")
                             )
        self.df_author_explode.show(20,truncate=False)
        print('********Query_3:first and last author ********')
        first_published_author = self.df_author_explode.orderBy(col("publish_date").asc(), col("created_time").asc()).head().author
        last_published_author = self.df_author_explode.orderBy(col("publish_date").desc(), col("created_time").desc()).head().author
        print('first_published_author:{}'.format(first_published_author))
        print('last_published_author:{}'.format(last_published_author))
        print('********Query_3:first and last author ********')
        print('********Query_4:Top 5 genres  ********')
        cleaned_books_genre_temp = (
            df_dqc.select('title', explode("genres").alias("genr"))
                .select("title", translate('genr', '.', '').alias("genre"))
        )

        cleaned_books_genre = (
            cleaned_books_genre_temp.groupBy("genre")
                .agg(count("title").alias("count_title"))
                .orderBy('count_title', ascending=False).limit(5)
        )
        cleaned_books_genre.show()

        print('********Query_4:Top 5 genres  ********')

        print('********Query_5:top 5 authors who (co-)authored the most books  ********')

        windowSpec = Window.partitionBy("title")
        df_unque_title_author = self.df_author_explode.select('title', 'author').distinct()

        df_title_co_authors = (
            df_unque_title_author
                .withColumn("cnt", count("author").over(windowSpec))
                .filter(col("cnt") > 1)
        )

        df_top_authors_co_authors = (
            df_title_co_authors.groupBy("author")
                .agg(countDistinct("title").alias("count_title"))
                .orderBy('count_title', ascending=False).limit(5)
        )
        df_top_authors_co_authors.show()
        print('********Query_5:top 5')

        print('********Query_6:Per publish year, the number of authors  ********')
        df_authors_per_year = (
            self.df_author_explode.groupBy("publish_date").agg(countDistinct("author"))
                .orderBy('publish_date')
        )
        df_authors_per_year.show(1000)

        print('********Query_6:****')

        print('********Query_6:The number of authors and number of books published per month between 1950 and 1970 ********')

        self.df_month_stat = (
            self.df_author_explode
                .filter("publish_date between 1950 and 1970")
                .withColumn("mon", month("created_time"))
                .groupBy("mon", "publish_date").agg(countDistinct("author").alias("author_cnt")
                                                    , countDistinct("title").alias("title_count")).orderBy(
                "publish_date", "mon")
        )
        self.df_month_stat.show(1000)
        print('********Query_6:Per publish year, the number of authors  ********')

        print(
            '********Query_7:No of pages written by an author for a book ********')

        self.df_pages_authors = (
                                self.df_author_explode.groupBy("author", "title").agg(max("number_of_pages").alias("number_of_pages"))
        )

        self.df_pages_authors.show(100)
        print('********Query_7:No of pages written by an author for a book ********')

    def unload_data(self):
        """Unloading Transformed  dataset.
          For this assignment I took Postgres as Target DB because of ease of use
          Depends on use case we can configure to anything
        """
        user = os.environ['POSTGRES_ETL_USER']
        password = os.environ['POSTGRES_ETL_PASSWORD']
        db = os.environ['POSTGRES_ETL_DB']
        host = os.environ['POSTGRES_ETL_HOST']

        url = "jdbc:postgresql://{}:5432/{}".format(host, db)

        print("######################################")
        print("LOADING POSTGRES TABLES")
        print("######################################")

        (
            self.df_author_explode.write
                .mode("overwrite")
                .format("jdbc")
                .option("url", url)
                .option("dbtable", 'public.author_title')
                .option("user", user)
                .option("password", password)
                .save()
        )

        (
            self.df_month_stat.write
                .mode("overwrite")
                .format("jdbc")
                .option("url", url)
                .option("dbtable", 'public.df_month_stat')
                .option("user", user)
                .option("password", password)
                .save()
        )
        (
            self.df_pages_authors.write
                .mode("overwrite")
                .format("jdbc")
                .option("url", url)
                .option("dbtable", 'public.authors_pages')
                .option("user", user)
                .option("password", password)
                .save()
        )

    def __del__(self):
        """
        Cleaning up all the created DF
        """
        print("Destructor called, CleanUP started.....")
        del self.df
        del self.df_author_explode
        del self.df_month_stat
        del self.df_pages_authors
        gc.collect()



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file",required=True, help=" full path for input file")

    args = parser.parse_args()
    main(args.input_file)