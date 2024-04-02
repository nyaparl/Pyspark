from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [('A','2023-01-15',100),
('B','2023-01-20',150),
('A','2023-02-10',120),
('B','2023-02-15',180),
('C','2023-03-05',200),
('A','2023-03-10',250),]
cols = ['pid','sdate','amt']

# Generate the data in the file
genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/practice/input.csv')
df.show(truncate=False)

df.createOrReplaceTempView('table')

spark.sql("WITH CTE as (select *,cast(month(sdate) as int) mnth from table) \
                 select * from CTE ").show()

spark.sql("WITH CTE1 as (select *,cast(month(sdate) as int) mnth from table), \
                CTE2 as (select *, row_number() over (partition by mnth order by amt desc) rn from CTE1) \
                select *from CTE2 where rn = 1").show()



