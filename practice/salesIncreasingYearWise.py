from sessionCreation import *

# Spark session created.
spark = sparkSession()

l2 = [ (1, 'Laptops', 'Electronics'),
 (2, 'Jeans', 'Clothing'),
 (3, 'Chairs', 'Home Appliances')]

l1 = [ (1, 2019, 1000.00),
 (1, 2020, 1200.00),
 (1, 2021, 1100.00),
 (2, 2019, 500.00),
 (2, 2020, 600.00),
 (2, 2021, 900.00),
 (3, 2019, 300.00),
 (3, 2020, 450.00),
 (3, 2021, 400.00)]

cols2 = ['pid','pname','cat']
cols1 = ['id','pyear','sales',]

# Generate the data in the file
genearateInputFile(l1,cols1)

# Requirement as below
# Write SQL query to the total sales for each day and also calculate the running sum only if we have \
#       consecutive dates

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/practice/input.csv')

df.createOrReplaceGlobalTempView('table')

spark.sql("select * from global_temp.table ").show()

df0 = spark.sql(" WITH CTE as (select *, lag(sales,1,0) over(partition by id order by pyear) lagsale, \
                     sales - lag(sales,1,0) over(partition by id order by pyear) nextyearsale \
                    from global_temp.table) \
                select id from CTE where id not in (select id from CTE where nextyearsale < 0 ) group by id ")

df0.show()


# Generate the data in the file
genearateInputFile(l2,cols2)

df1 = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/User/PycharmProjects/Pyspark/practice/input.csv')


df1.filter(df1.pid == 2).show()

