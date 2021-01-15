import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F

#1 Start a simple Spark Session
spark = SparkSession.builder\
                .master("local[*]")\
                .appName("Walmart_Stock")\
                .getOrCreate()
                
 #2 Load the Walmart Stock CSV File
df = spark.read\
        .option("header",True)\
        .csv("walmart_stock.csv
 df.show(3)
 
 #3 column names
df.columns

# 4 schema look?
df.printSchema()

#5 New column HV_Ratio =Ratio High price/ Volume of stock per day
df2= df.withColumn("HV_Ratio", col("High")/col("Volume"))
df2.show(3)

#6 What Day had the peak high price?
df.orderBy(col("High").desc())\
    .select(col("Date"))\
    .head(3)
 
 #6 What Day had the peak high price? SQL
df.createOrReplaceTempView("stock")
spark.sql(""" 
select Date 
from stock
order By High desc """).show(1)

#7 what is the mean of the Close column?
df.createOrReplaceTempView("stock")
spark.sql("""
select avg(Close) as mean_Close
from stock 
""").show()

#7 what is the mean of the Close column?
df.select(col("Close")).agg(avg("Close")).show()

#8 What is the max and min of the volumn column?
df.createOrReplaceTempView("stock")
spark.sql("""
select min(Volume), max(Volume)
from stock 
""").show()

#8 What is the max and min of the volumn column? (spark)
df.select(col("Volume")).agg(min("Volume"), max("Volume")).show()

#9 How many days was the CLose lower than 60 dollars?
df.createOrReplaceTempView("stock")
spark.sql("""
select count(Close)  as jour_close_inf_60
from stock
where Close < 60

""").show()

## 9) How many days was the Close lower than 60 dollars? (spark)
df.filter(df['Close'] < 60).count()

#10 percentage of the time was the High greater than 80 dollars 
spark.sql(""" 
select count(Date)*100/(select count(*) from stock) as Pourcentage
from stock
where High > 80""").show()

#10 percentage of the time was the High greater than 80 dollars (spark)
df.filter('High > 80').count() * 100/df.count()

# 11 Max high per year
df.createOrReplaceTempView("stock")
spark.sql("""
SELECT substr(Date,1,4) as year, max(High )
FROM stock
group by year
order by year

""").show()

# 11 Max high per year (Spark)
df.groupby(year("date")).agg(max("high")).orderBy(year("date")).show()

# 12  average Close for each calendar Month
df.createOrReplaceTempView("stock")
spark.sql("""
SELECT substr(Date,6,2) as month, avg(Close ) as avg_close_per_month
FROM stock
group by month
order by month

""").show()

# 12  average Close for each calendar Month (Spark)
df.groupby(month("date")).agg(avg("Close")).orderBy(month("date")).show()

