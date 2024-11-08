lakehousename = "LakehouseForTesting"

%pip install semantic-link-labs

import sempy.fabric as fabric
import pandas as pd
from pyspark.sql.functions import col, last_day, dayofweek, year, month, date_format, rand, randn, expr
import time
import sempy.fabric as fabric
import sempy_labs as labs
from sempy_labs import migration, directlake, admin
from sempy_labs import lakehouse as lake
from sempy_labs import report as rep
from sempy_labs.tom import connect_semantic_model
from sempy_labs.report import ReportWrapper
from sempy_labs import ConnectWarehouse
from sempy_labs import ConnectLakehouse

########## create lakehouse ##########
lakehouse_id = fabric.create_lakehouse(lakehousename)
workspace_id = fabric.get_workspace_id()

########## add data ##########
# How many rows should be in the fact table?
num_rows_in_facts = 10000
# How many fact tables do you want in your model?
num_fact_tables = 1

# Let's create a date table!
start_date = '2020-01-01'
end_date = '2023-12-31'
years = 4

date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='Date')
date_df['Date'] = date_df['Date'].astype(str)

spark_df = spark.createDataFrame(date_df)
spark_df = spark_df.withColumn('Date', col('Date').cast('date'))
spark_df = spark_df.withColumn('DateID', date_format(col('Date'),"yyyyMMdd").cast('integer'))
spark_df = spark_df.withColumn('Monthly', date_format(col('Date'),"yyyy-MM-01").cast('date'))
spark_df = spark_df.withColumn('Month', date_format(col('Date'),"MMM"))
spark_df = spark_df.withColumn('MonthYear', date_format(col('Date'),"MMM yyyy"))
spark_df = spark_df.withColumn('MonthOfYear', month(col('Date')))
spark_df = spark_df.withColumn('Year', year(col('Date')))
spark_df = spark_df.withColumn('EndOfMonth', last_day(col('Date')).cast('date'))
spark_df = spark_df.withColumn('DayOfWeekNum', dayofweek(col('Date')))
spark_df = spark_df.withColumn('DayOfWeek', date_format(col('Date'),"EE"))
spark_df = spark_df.withColumn('WeeklyStartSun', col('Date')+1-dayofweek(col('Date')))
spark_df = spark_df.withColumn('WeeklyStartMon', col('Date')+2-dayofweek(col('Date')))
spark_df.show()

delta_table_name = "date"
filePath = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{delta_table_name}"
spark_df.write.format("delta").save(filePath)

# A table to store the measures in the model
data = [(1, 'Measures only')]
columns = ['ID', 'Col1']

measure_df = spark.createDataFrame(data, columns)
measure_df.show()

delta_table_name = "measuregroup"
filePath = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{delta_table_name}"
measure_df.write.format("delta").save(filePath)

# A small dimension table
data = [(1, 'Accessories'), (2, 'Bikes'), (3, 'Clothing')]
columns = ['ProductCategoryID', 'ProductCategory']

product_df = spark.createDataFrame(data, columns)
product_df.show()

delta_table_name = "product"
filePath = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{delta_table_name}"
product_df.write.format("delta").save(filePath)


# a larger dimension table, with USA and Australia and their states/territories
data = [(1, 'Alabama', 'USA'),(2, 'Alaska', 'USA'),(3, 'Arizona', 'USA'),(4, 'Arkansas', 'USA'),(5, 'California', 'USA'),(6, 'Colorado', 'USA'),(7, 'Connecticut', 'USA'),(8, 'Delaware', 'USA'),(9, 'Florida', 'USA'),(10, 'Georgia', 'USA'),(11, 'Hawaii', 'USA'),(12, 'Idaho', 'USA'),(13, 'Illinois', 'USA'),(14, 'Indiana', 'USA'),(15, 'Iowa', 'USA'),(16, 'Kansas', 'USA'),(17, 'Kentucky', 'USA'),(18, 'Louisiana', 'USA'),(19, 'Maine', 'USA'),(20, 'Maryland', 'USA'),(21, 'Massachusetts', 'USA'),(22, 'Michigan', 'USA'),(23, 'Minnesota', 'USA'),(24, 'Mississippi', 'USA'),(25, 'Missouri', 'USA'),(26, 'Montana', 'USA'),(27, 'Nebraska', 'USA'),(28, 'Nevada', 'USA'),(29, 'New Hampshire', 'USA'),(30, 'New Jersey', 'USA'),(31, 'New Mexico', 'USA'),(32, 'New York', 'USA'),(33, 'North Carolina', 'USA'),(34, 'North Dakota', 'USA'),(35, 'Ohio', 'USA'),(36, 'Oklahoma', 'USA'),(37, 'Oregon', 'USA'),(38, 'Pennsylvania', 'USA'),(39, 'Rhode Island', 'USA'),(40, 'South Carolina', 'USA'),(41, 'South Dakota', 'USA'),(42, 'Tennessee', 'USA'),(43, 'Texas', 'USA'),(44, 'Utah', 'USA'),(45, 'Vermont', 'USA'),(46, 'Virginia', 'USA'),(47, 'Washington', 'USA'),(48, 'West Virginia', 'USA'),(49, 'Wisconsin', 'USA'),(50, 'Wyoming', 'USA'),(51, 'New South Wales', 'Australia'),(52, 'Queensland', 'Australia'),(53, 'South Australia', 'Australia'),(54, 'Tasmania', 'Australia'),(55, 'Victoria', 'Australia'),(56, 'Western Australia', 'Australia'),(57, 'Australian Capital Territory', 'Australia'),(58, 'Northern Territory', 'Australia')]
columns = ['GeoID', 'StateOrTerritory', 'Country']

geo_df = spark.createDataFrame(data, columns)
geo_df.show()

delta_table_name = "geo"
filePath = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{delta_table_name}"
geo_df.write.format("delta").save(filePath)


# This one has no upward trending

# loop to create fact tables with random links to the above dimensions
# and a sales column with random generated numbers (1-1000)
# and a costs column with random generated numbers (1-100)

dfs = []
for i in range(1, num_fact_tables+1):
    df = spark.range(0, num_rows_in_facts).withColumn('ProductCategoryID', (rand(seed=i)*3+1).cast('int')).withColumn('GeoID', (rand(seed=i)*58+1).cast('int')).withColumn('DateID', expr('cast(date_format(date_add("2020-01-01", cast(rand(100) * 365 * 4 as int)), "yyyyMMdd") as int)')).withColumn('Sales', (rand(seed=i*4)*1000+1).cast('int')).withColumn('Costs', (rand(seed=i+45)*100+1).cast('int'))
    dfs.append(df)
    df_name = 'df_{}'.format(i)
    globals()[df_name] = df
    print("Name is sales_{} with {} rows.".format(i, df.count()))
    delta_table_name = 'sales_{}'.format(i)
    filePath = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{delta_table_name}"
    df.write.format("delta").save(filePath)
