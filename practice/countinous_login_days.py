from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [(1, '2024-03-01'),
(1, '2024-03-02'),
(1, '2024-03-03'),
(1, '2024-03-04'),
(1, '2024-03-06'),
(1, '2024-03-10'),
(1, '2024-03-11'),
(1, '2024-03-12'),
(1, '2024-03-13'),
(1, '2024-03-14'),
(1, '2024-03-20'),
(1, '2024-03-25'),
(1, '2024-03-26'),
(1, '2024-03-27'),
(1, '2024-03-28'),
(1, '2024-03-29'),
(1, '2024-03-30'),
(2, '2024-03-01'),
(2, '2024-03-02'),
(2, '2024-03-03'),
(2, '2024-03-04'),
(3, '2024-03-01'),
(3, '2024-03-02'),
(3, '2024-03-03'),
(3, '2024-03-04'),
(3, '2024-03-04'),
(3, '2024-03-04'),
(3, '2024-03-05'),
(4, '2024-03-01'),
(4, '2024-03-02'),
(4, '2024-03-03'),
(4, '2024-03-04'),
(4, '2024-03-04')]
cols = ['user','cdate']

# Generate the data in the file
genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/practice/input.csv')
df.show(truncate=False)

df = df.withColumn('date1',col("cdate").cast("date")).orderBy(col("user"),col("date1"))

df.createOrReplaceTempView('temp')

spark.sql("select user, date1, row_number() over(partition by user order by date1) as rn \
          from temp").show(50)

spark.sql("With CTE1 as (select user, date1, row_number() over(partition by user order by date1) as rn \
          from temp), \
          CTE2 as (Select c.*, dateadd(day,rn*-1,date1) as dte1 from CTE1 c) \
          select user, min(date1) as start_date, max(date1) end_date, count(rn) from CTE2 group by user,dte1 \
          having count(rn) >= 5 ").show()











