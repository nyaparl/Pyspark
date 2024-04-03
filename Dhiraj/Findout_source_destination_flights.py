from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),
 (1,'Flight2' , 'Hyderabad' , 'Kochi'),
 (1,'Flight3' , 'Kochi' , 'Mangalore'),
 (2,'Flight1' , 'Mumbai' , 'Ayodhya'),
 (2,'Flight2' , 'Ayodhya' , 'Gorakhpur'),
 (3, 'Flight2', 'chennai', 'bangalore')     ]
cols = ['id','fid','frm','to']

# Generate the data in the file
genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/Dhiraj/input.csv')
df.show(truncate=False)

df.createOrReplaceTempView('temp')

spark.sql("With CTE1 as (select *, Case when frm not in (select t.to from temp t where t.id = id ) then frm else NULL end as start, \
                     Case when to not in (select t.frm from temp t where t.id = id ) then to else NULL end as end \
           from temp) \
            select id, first(start), last(end) from CTE1 group by id").show()


