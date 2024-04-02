import os
import csv
from pyspark.sql import SparkSession

print(os.getcwd())
spark = SparkSession.builder.appName('first') \
                    .master('local[*]').getOrCreate()


l1 = [(1,'I love to play cricket'),(2,'I am into motorbiking'),(3,'What do you like')]
cols = ['id','text']

if os.path.exists("input.csv"):
    os.remove("input.csv")

with open("input.csv", 'w', newline='') as file:
    writer = csv.writer(file)

    writer.writerow(cols)

    for e in l1:
        writer.writerow(list(e))

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/practice/input.csv')
df.show(truncate=False)

