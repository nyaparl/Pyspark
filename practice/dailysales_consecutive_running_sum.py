from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [(1, 100, 10000, '2024-07-01'),
(2, 100, 20000, '2024-07-02'),
(3, 100, 30000, '2024-07-05'),
(4, 300, 25000, '2024-07-07'),
(5, 500, 50000, '2024-07-02'),
(6, 200, 12000, '2024-07-01')]

cols = ['cid','pid','sales','sdate']

# Generate the data in the file
genearateInputFile(l1,cols)

# Requirement as below
# Write SQL query to the total sales for each day and also calculate the running sum only if we have \
#       consecutive dates

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/practice/input.csv')
df.show(truncate=False)

df.createOrReplaceTempView('table')

spark.sql("WITH CTE1 as (select *,cast(sdate as date) date1  from table), \
                CTE2 as (select date1, sum(sales) sales, row_number() over( order by date1 ) as rn, \
                     dateadd(DAY, rn * -1, date1) cdate from CTE1 group by date1) \
                 select *, sum(sales) over(partition by cdate order by date1) runsales from CTE2 order by date1 ").show()




