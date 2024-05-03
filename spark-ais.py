from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# Function to distingush if it is a ship
def isShip(mmsi):
    mmsi0= int(mmsi[0]);
    return mmsi0>=2 and mmsi0<=7;

# Make udf
udf_isShip= udf(isShip,BooleanType());

# Create session
spark = SparkSession \
    .builder \
    .appName("SparkTypeCount") \
    .master("yarn") \
    .getOrCreate();

# This does not work
spark.sparkContext.setLogLevel("WARN");

# Read ship types
df_types= spark.read.format("csv")\
                  .option("header","true")\
                  .option("inferSchema","true")\
                  .load("s3://ubs-datasets/ais/ship_types.csv");

#df_types.printSchema();
#df_types.show(100);

# Define variables data types of the schema for AIS dataset
mmsi_field=StructField("MMSI",StringType(),True);
time_field=StructField("TIME",StringType(),True);
latitude_field=StructField("LATITUDE",DoubleType(),True);
longitude_field=StructField("LONGITUDE",DoubleType(),True);
cog_field=StructField("COG",FloatType(),True);
sog_field=StructField("SOG",FloatType(),True);
heading_field=StructField("HEADING",DoubleType(),True);
navstat_field=StructField("NAVSTAT",ShortType(),True);
imo_field=StructField("IMO",StringType(),True);
name_field=StructField("NAME",StringType(),True);
callsign_field=StructField("CALLSIGN",StringType(),True);
type_field=StructField("TYPE",ShortType(),True);

ais_field_list=[mmsi_field,time_field,latitude_field,
                longitude_field,cog_field,sog_field,
                heading_field,navstat_field,imo_field,
                name_field,callsign_field,type_field];

# Define schema
ais_schema= StructType(ais_field_list);

# Read ship types
df_ais= spark.read.format("csv")\
                  .option("header","false").schema(ais_schema)\
                  .load("s3a://ubs-datasets/ais/clean/2021/part-r-00000");

# Select data that we will need, # drop duplicates later
df_ships= df_ais.select("MMSI","NAME","TYPE","LATITUDE", "LONGITUDE")\
                .filter(udf_isShip(df_ais.MMSI))\
                .distinct();
                
# Filter ships that apporached Roterdam based on lat and lon
df_roterdam = df_ships.filter((col("LATITUDE")<52.09) & (col("LATITUDE")>51.78) & (col("LONGITUDE")>3.8) & (col("LONGITUDE")<4.93))
df_roterdam.show()

# Check if the number of ships is lower compared to the initial number, it is also shown using show() in the previous step that only the Roterdam area is presented
print(f'Total ships: {df_ships.count()}')
print(f'Roterdam ships: {df_roterdam.count()}')

# Filter ships that apporached Shangai based on lat and lon
df_shangai = df_ships.filter((col("LATITUDE")<35.35) & (col("LATITUDE")>29.7) & (col("LONGITUDE")>120.52) & (col("LONGITUDE")<123.8))
df_shangai.show()

# Check if the number of ships is lower compared to the initial number, it is also shown using show() in the previous step that only the Shangai area is presented
print(f'Total ships: {df_ships.count()}')
print(f'Shangai ships: {df_shangai.count()}')

# Now that we have two datasets we will drop duplicates and then create another table in which we will join all ships that are found close to Roterdam and close to Shangai

df_roterdam_unique = df_roterdam.dropDuplicates(["MMSI"])
df_shangai_unique = df_shangai.dropDuplicates(["MMSI"])
df_roterdam_unique.show()
df_shangai_unique.show()

# Check duplicates removal
print(f'Roterdam ships: {df_roterdam.count()}')
print(f'Roterdam ships removed duplicates: {df_roterdam_unique.count()}')
print(f'Shangai ships: {df_shangai.count()}')
print(f'Shangai ships removed duplicates: {df_shangai_unique.count()}')


# Now join two dataframes based on their mmsi to obtain only ships that were in Roterdam and Shangai
df_first_join = df_roterdam_unique.join(df_shangai_unique,"MMSI")
df_first_join.show()

# Now we join the types of ships to my df_types_join to obtain final result
df_types_join= df_first_join.join(df_types,df_first_join.TYPE==df_types.TYPE,"right")\
                       .select(df_types.TYPE,"TYPENAME");

df_types_join.show(100,truncate=False);


df_types_count= df_types_join.groupBy("TYPE","TYPENAME")\
                             .count()\
                             .orderBy("TYPE");

df_types_count.show(101,truncate=False);

df_types_count.coalesce(1).write.format("csv").mode("overwrite").save("s3a://ubs-cde/home/e2208169/output-type_count")


