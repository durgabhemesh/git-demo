from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time


def print_hi(name):
    my_schema = StructType([StructField("orderid", IntegerType()), StructField("time", StringType()),
                            StructField(("customer_id"), IntegerType()), StructField(("status"), StringType())])

    df1 = name.read.format("csv").option("path", "C:\\Users\\durga\\OneDrive\Desktop\\orders-201019-002101.csv") \
        .option("header", True) \
        .schema(my_schema) \
        .load()

    df1.printSchema()

    print(df1.rdd.getNumPartitions())

    # df2=df1.select("customer_id", "status") \
    #     .where("status == 'CLOSED' ") \
    #     .groupBy("customer_id") \
    #     .count()

    df1.createOrReplaceTempView("orders")


    name.sql("select customer_id, count(*) as total_orders from orders  where status='CLOSED' group by customer_id ").show()
    time.sleep(20)
    # df = name.read.option("header", True).csv("C:\\Users\\durga\\OneDrive\Desktop\\orders-201019-002101.csv") \
    #     .where("order_id < 2 ") \
    #     .select(["order_id", "order_status"]) \
    #     .groupBy("order_status") \
    #     .count()
    #
    # # df2=df.printSchema()
    # # df2.show()
    #
    # df.show()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = SparkSession.builder.appName("he").getOrCreate()

    print_hi(spark)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
