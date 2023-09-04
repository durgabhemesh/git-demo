from pyspark.sql import SparkSession
from pyspark.sql.functions import *



def word_count(spark):
    df=spark.read.text("C:\\Users\\durga\\OneDrive\\Desktop\\practise_input.txt")

    df.withColumn("word",explode(split("value"," "))).groupBy("word").count().show()
    #df.withColumn("word", explode(split("value", " "))).show()
def popular_movies(spark):
    df = spark.read.format("csv") \
    .option("path","C:\\Users\\durga\\OneDrive\\Desktop\\ratings_small.csv") \
    .option("inferSchema",True) \
    .option("header",True) \
    .load()

    movies=spark.read.format("csv") \
    .option("path","C:\\Users\\durga\\OneDrive\\Desktop\\movies_metadata.csv") \
    .option("inferSchema",True) \
    .option("header",True) \
    .load()
    df3=df.groupBy("movieId").count().sort(desc("count"))
    df.createOrReplaceTempView("surrendra")
    spark.sql("select movieId,count(userId) from surrendra group by movieId ").show()
    df3.show()
    df2=df.groupBy("movieId").agg(count("userId")).withColumnRenamed("count(userId)","ratings").sort(desc("ratings")).repartition(6)
    #df2.show()

    print(spark.sparkContext.defaultParallelism)
    print('numparitions',df2.rdd.getNumPartitions())
    most_popular=df2.join(movies,df2.movieId == movies.id)
    most_popular_movies=most_popular.select("movieId","original_title")

    #most_popular_movies.show(10,truncate=False)
    input('')

if __name__=="__main__":
    spark=SparkSession.builder.appName("word count").master("local[4]").getOrCreate()
    popular_movies(spark)
    #word_count(spark)


