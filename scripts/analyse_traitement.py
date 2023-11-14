from extract import *

#print("count start ####################################################################")
print(df1.count())
#print("count end ####################################################################")
print(df2.count())

#print("distinct start####################################################################")
print(df1.distinct().count())
#print("distinct end####################################################################")
print(df2.distinct().count())

#print("####################################################################")
df1.printSchema()
#print("####################################################################")
df2.printSchema()

#print("####################################################################")
df1.show()
#print("####################################################################")
df2.show()

df1 = df1.dropDuplicates()
print(df1.count())

df1.describe('DepDelay', 'ArrDelay').show()

df1.select('Carrier').distinct().show()

df1.filter(df1["ArrDelay"] > 30).show()

df1.groupBy(['ArrDelay']).agg({'DestAirportID':'count'}).show()

join_by_id = df1.join(df2, df1.OriginAirportID == df2.airport_id).groupby('city').count()
join_by_id.show()

new_df1 = df1.select('DayofMonth', 'DayOfWeek', 'Carrier', 'OriginAirportID', 'DestAirportID', 'DepDelay', ((col('ArrDelay') > 10).cast("Int").alias("Label")))


#on divise notre dataset en train et test:
data_split = new_df1.randomSplit([0.8, 0.2])
train_set = data_split[0]
test_set = data_split[1].withColumnRenamed("Label", "True_Label")
