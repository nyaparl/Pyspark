from sessionCreation import *

# Spark session created.
spark = sparkSession()

df = spark.read.format('json').option('multiline','true') \
                              .load('C:/Users/User/PycharmProjects/Pyspark/Dhiraj/json-data.json')

df.show()
df.printSchema()
df = df.withColumn("branch_data",explode(col("branches")))
df.select(col("Course_type"), col("Head_Office_Contact"), col("Institute_Name"), col("branch_data.*")).show()



df.createOrReplaceTempView('temp')

# spark.sql("select Course_type,Head_Office_Contact, Institute_Name, explode(array(branches)) from temp").show(truncate=False)