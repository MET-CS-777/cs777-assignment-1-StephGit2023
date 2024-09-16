##... CS777 Big Data Analytics 
#
##... Assignment 1
#
##... 09/13/24



# - Relevant Libraries...
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys

# - Exception handling determines if the value can be converted to float...
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

# - Defined function cleans and validates each row...
def cleanRows(p):
    # - Verifies each tuple has 17 attributes...
    if len(p) == 17:  
        if isfloat(p[5]) and isfloat(p[11]) and isfloat(p[4]) and isfloat(p[16]):
            # - ( If trip duration > 60 seconds ), ( trip distance > 0 ), ( fare > $0.1 ), ( total_amount > $0.1 )... 
            if float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0:
                return True
    return False

if __name__ == "__main__":
    # - Checks if the correct number of command-line arguments are provided...
    if len(sys.argv) != 4:
        print("Usage: Laleye_Stephan_Assignment_1.py <input_file> <output_file_task1> <output_file_task2>", file=sys.stderr)
        sys.exit(-1)

# - Initializes SparkContext and SparkSession...
sc = SparkContext(appName="TaxiDataApp")
spark = SparkSession.builder.getOrCreate()

# - Loads the dataset from Google Cloud Storage (GCS) using the provided URL...
file_url = sys.argv[1]
# - The GCS URL is passed as the first argument...
taxi_rdd = sc.textFile(file_url)

# - Splits each line by the comma and cleans the data...
taxi_rdd = taxi_rdd.map(lambda line: line.split(','))
cleaned_rdd = taxi_rdd.filter(cleanRows)

# - ( Task 4.1): Computes the top 10 taxis with most distinct drivers...
top_10_taxis = (
    cleaned_rdd
    .map(lambda row: (row[0], row[1]))
    # - (medallion, hack_license) tuples...
    .distinct()
    # - Keep only all distinct (medallion, hack_license) pairs...
    .map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
    # - Count number of drivers per taxi
    .takeOrdered(10, key=lambda x: -x[1])
    # - Retrieve the top 10 taxis by driver count...
    )


# - Saves ( Task 4.1 ) results to the specified output file...
sc.parallelize(top_10_taxis).coalesce(1).saveAsTextFile(sys.argv[2])



# - ( Task 4.2): Top 10 drivers by average earnings per minute...
top_10_drivers = (
    cleaned_rdd
    .map(lambda row: (row[1], (float(row[16]), float(row[4]) / 60)))
    # - ( hack_license, ( total_amount, trip_time_in_minutes ))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    # - Sum earnings and time for each driver...
    .mapValues(lambda x: x[0] / x[1])
    # - Calculates the average earnings per minute...
    .takeOrdered(10, key=lambda x: -x[1])
    # - Retrieves the top 10 drivers by average earnings per minute...
    )


# - Saves the ( Task 4.2 ) results to the specified output file...
sc.parallelize(top_10_drivers).coalesce(1).saveAsTextFile(sys.argv[3])


# - Stop the Spark session
sc.stop()
