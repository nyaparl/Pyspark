from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [(1,'I love to play cricket'),(2,'I am into motorbiking'),(3,'What do you like')]
cols = ['id','text']

# Generate the data in the file
genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/practice/input.csv')
df.show(truncate=False)



