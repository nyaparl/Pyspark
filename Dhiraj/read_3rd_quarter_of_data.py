from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [('Alice', 28),
 ('Bob', 35),
 ('Charlie', 42),
 ('David', 25),
 ('Eva', 31),
 ('Frank', 38),
 ('Grace', 45),
 ('Henry', 29)]
cols = ['name','age']

# Generate the data in the file
genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/Dhiraj/input.csv')
df.show(truncate=False)

tot_rec = df.count()
each_quarter_recs = tot_rec / 4
quarter_start_recs = (each_quarter_recs * 3) -1
quarter_end_recs = (quarter_start_recs + each_quarter_recs) -1

print('tot_rec ==> {} '.format(tot_rec) )
print('each_quarter_recs  ==> {} '.format(each_quarter_recs))
print('quarter_start_recs ==> {} '.format(quarter_start_recs))
print('quarter_end_recs  ==> {} '.format(quarter_end_recs))

df.createOrReplaceTempView('temp')

spark.sql(" with CTE1 as (select *,row_number() over(order by 'A' ) as rn from temp)  \
           select * from CTE1 where rn between {} and {} ".format(quarter_start_recs,quarter_end_recs)).show()

