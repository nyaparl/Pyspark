from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [(1,'Abbot'),(2,'Doris'),(3,'Emerson'),(4,'Green'),(5,'jeames')]
cols = ['sno','name']

# Generate the data in the file
genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/Dhiraj/input.csv')
df.show(truncate=False)

df.createOrReplaceTempView('temp')

spark.sql("With CTE1 as (select t.*, lead(name) over(order by sno) as nxt_user, \
          lag(name) over(order by sno) prv_user from temp t) \
          select sno, Case When sno % 2 != 0 Then coalesce(nxt_user,name) else prv_user end as user from CTE1").show()