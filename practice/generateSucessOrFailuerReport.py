from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [('2018-12-28','F'),('2018-12-29','F'),('2019-01-04','F'),('2019-01-05','F'),
('2018-12-30','S'),('2018-12-31','S'),('2019-01-01','S'),('2019-01-02','S'),
('2019-01-03','S'),('2019-01-06','S')]
cols = ['dte','sts']

# Generate the data in the file
genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/practice/input.csv')
df.show(truncate=False)

df = df.withColumn("dte", col("dte").cast('date'))
df.printSchema()

df.createOrReplaceTempView('table')

spark.sql("WITH CTE1 as (Select *, datediff(day, dte,lead(dte,1,dte) over(order by dte)) as dif \
          from table where dte >= '2019-01-01' and dte <= '2019-12-31' order by dte) \
          select sts,dte,dif, lag(sts,1,sts) over(order by dte) as psts, \
            sum(CASE WHEN sts = lag(sts,1,sts) over(order by dte) then 0 else 1 end) over(order by dte) as flag From CTE1 ").show()




