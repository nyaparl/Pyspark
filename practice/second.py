from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('first') \
                    .master('local[*]').getOrCreate()

print(spark)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('D:/Data/test123.csv')
df.show()

