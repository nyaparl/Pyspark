from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('first') \
                    .master('local[*]').getOrCreate()

print(spark)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('D:/Data/test123.csv')
df.show()

# dept = [("Finance",10),
#         ("Marketing",20),
#         ("Sales",30),
#         ("IT",40) ]
#
# deptColumns = ["dept_name","dept_id"]
# deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
# deptDF.printSchema()
# deptDF.show(truncate=False)