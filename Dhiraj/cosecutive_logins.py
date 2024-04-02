from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [(101,'02-01-2024','N'),
(101,'03-01-2024','Y'),
(101,'04-01-2024','N'),
(101,'07-01-2024','Y'),
(102,'01-01-2024','N'),
(102,'02-01-2024','Y'),
(102,'03-01-2024','Y'),
(102,'04-01-2024','N'),
(102,'05-01-2024','Y'),
(102,'06-01-2024','Y'),
(102,'07-01-2024','Y'),
(103,'01-01-2024','N'),
(103,'04-01-2024','N'),
(103,'05-01-2024','Y'),
(103,'06-01-2024','Y'),
(103,'07-01-2024','N')]
cols = ['id','dte','flag']

# Generate the data in the file
genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/Dhiraj/input.csv')
df.show(truncate=False)
df = df.withColumn('dte',to_date(col('dte'),'dd-MM-yyyy'))
df.show()
df.printSchema()

df.createOrReplaceTempView('temp')

spark.sql(" with CTE1 as (select *, dateadd(day, row_number() over(partition by id order by dte) * -1 ,dte) as diff from temp \
          where flag = 'Y') select id, min(dte) as start_date, max(dte) as end_date, count(*) cons_days \
           from CTE1 group by id,diff having count(*) >= 2").show()

