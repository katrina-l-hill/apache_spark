from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo").getOrCreate()
#print(spark.version)

#Creating a Spark DataFrame

df = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", 13),
    ],
    ["first_name", "age"],
)

df.show()

# Adding a column to a Spark DataFrame

from pyspark.sql.functions import col, when

df1 = df.withColumn(
    "life_stage",
    when(col("age") < 13, "child")
    .when(col("age").between(13, 19), "teenager")
    .otherwise("adult"),
)

df1.show()

# Filtering a Spark DataFrame

df1.where(col("life_stage").isin(["teenager", "adult"])).show()

# Grouping by aggregation on Spark DataFrame

from pyspark.sql.functions import avg

df1.select(avg("age")).show()

# Querying the DataFrame with SQL

spark.sql("select avg(age) from {df1}", df1=df1).show()

