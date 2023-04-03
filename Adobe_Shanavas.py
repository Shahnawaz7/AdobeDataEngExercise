# Databricks notebook source
# DBTITLE 1,Initialise the spark libraries
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------

# DBTITLE 1,Read Source File
src_df=spark.read.format("csv").option("delimiter","\t").option("header", True).load("dbfs:/FileStore/tables/data.tsv")
src_df=src_df.select(src_df.hit_time_gmt.cast(IntegerType()), "ip", "user_agent", "product_list", "referrer")

# COMMAND ----------

# DBTITLE 1,Preprocessing the input data with revenue
revenue_record_df=src_df.filter("pagename=='Order Complete'")
product_details_without_explode_df=revenue_record_df.withColumn("product_detail", split(col("product_list"),","))
product_details_derived_df=product_details_without_explode_df.select("hit_time_gmt","user_agent", "ip",explode("product_detail").alias("product_sale"))
product_details_revenue_df=product_details_derived_df.withColumn("revenue", split(col("product_sale"),";")[2].cast('int') * split(col("product_sale"),";")[3].cast('double'))
product_details_revenue_df.createOrReplaceTempView("revenue_record")
product_details_revenue_df.display()

# COMMAND ----------

# DBTITLE 1,Preprocessing the input data for lead
lead_df=src_df.filter("referrer not like '%esshopzilla%'")

lead_with_name_df=lead_df.withColumn("pre_url",split(col("referrer"),".com")[0]).withColumn("lead_base_name",element_at(split(col("pre_url"),"\."),-1))
lead_with_name_keyword_df=lead_with_name_df.withColumn("param_string", element_at(split(col("referrer"),"\?"),-1)).withColumn("keyword", expr("lower(CASE WHEN lead_base_name == 'yahoo' THEN  split(split(param_string,'p=')[1],'&')[0] ELSE split(split(param_string,'q=')[1],'&')[0] END)"))

lead_with_name_keyword_df=lead_with_name_keyword_df.select("hit_time_gmt", "user_agent","ip", "lead_base_name", "keyword")
lead_with_name_keyword_df.display()
lead_with_name_keyword_df.createOrReplaceTempView("lead_record")

# COMMAND ----------

# DBTITLE 1,Combine revenue and lead

lead_to_revenue_df=spark.sql("""select l.lead_base_name,l.keyword,SUM(r.revenue) as total_revenue from revenue_record r join  lead_record l 
            on r.user_agent=l.user_agent and r.ip=l.ip and r.hit_time_gmt - 3600 * 24 < l.hit_time_gmt 
            group by lead_base_name, keyword""")
lead_to_revenue_df.display()

# COMMAND ----------

# DBTITLE 1,Presentation to target
final_df=lead_to_revenue_df.select(concat("lead_base_name",lit(".com")).alias("Search Engine Domain"), col("keyword").alias("Search Keyword"), col("total_revenue").alias("Revenue"))
final_df.display()
final_df.repartition(1).sort(desc("Revenue")).write.format("csv").option("delimiter","\t").option("header", True).mode("overwrite").save("dbfs:/FileStore/adobe/result.tsv")
