import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import split, explode
spark = SparkSession.builder.getOrCreate()
raw_data_frame = spark.read.json("/home/nallani/hadoop-3.3.1/ipl/980963.json", multiLine = "true")
raw_data_frame.printSchema()


print("First Innings ")
inn_df1=raw_data_frame.select(f.col('innings').getItem(0).alias('innings1'))
inn_df1.show()

print("displaying overs")
overs_df1=inn_df1.select(explode('innings1.overs').alias('overs'))
overs_df1.show()

print("create a row for each element in the array ")
data_f1=overs_df1.select(explode('overs.deliveries').alias('deliveries'))
data_f1.show()

print("Displaying each batter and bowler count")
run_sc_1=data_f1.select('deliveries.batter','deliveries.bowler',f.col('deliveries.runs.batter').alias("runs_scored"))
run_sc_1.show()
print("Displaying total runs scored")
tot_run_1=run_sc_1.groupBy('batter','bowler').agg(f.sum('runs_scored').alias('total_runs_scored'))
tot_run_1.show()
print("Maximun runs scored")
max_run_sc1=tot_run_1.groupBy('batter').agg(f.max('total_runs_scored').alias('max_runs_scored'))
max_run_sc1.show()
print("maximun runs scored among all bowlers")
result1=tot_run_1.join(max_run_sc1, (max_run_sc1.max_runs_scored==tot_run_1.total_runs_scored) & (max_run_sc1.batter==tot_run_1.batter), how="leftsemi").withColumnRenamed("total_runs_scored", "max_runs_scored")
result1.show()

print("Second Innings ")
inn_df2=raw_data_frame.select(f.col('innings').getItem(1).alias('innings2'))
inn_df2.show()

print("displaying overs")
overs_df2=inn_df2.select(explode('innings2.overs').alias('overs'))
overs_df2.show()

print("create a row for each element in the array ")
data_f2=overs_df2.select(explode('overs.deliveries').alias('deliveries'))
data_f2.show()

print("Displaying each batter and bowler count")
run_sc_2=data_f2.select('deliveries.batter','deliveries.bowler',f.col('deliveries.runs.batter').alias("runs_scored"))
run_sc_2.show()

print("Displaying total runs scored")
tot_run_2=run_sc_2.groupBy('batter','bowler').agg(f.sum('runs_scored').alias('total_runs_scored'))
tot_run_2.show()

print("Maximun runs scored")
max_run_sc2=tot_run_2.groupBy('batter').agg(f.max('total_runs_scored').alias('max_runs_scored'))
max_run_sc2.show()

print("maximun runs scored among all bowlers")
result2=tot_run_2.join(max_run_sc2, (max_run_sc2.max_runs_scored==tot_run_2.total_runs_scored) & (max_run_sc2.batter==tot_run_2.batter), how="leftsemi").withColumnRenamed("total_runs_scored", "max_runs_scored")
result2.show()
