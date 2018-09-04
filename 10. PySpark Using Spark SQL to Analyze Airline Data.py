----------------------------------
Using Spark SQL to Analyze Airline Data:- Janani Ravi
----------------------------------

>>> from pyspark.sql import SparkSession

>>> from pyspark.sql.types import Row

>>> from datetime import datetime

#Take data from my github and Read Data from HDFS

>>> airlinesPath = "/airlines.csv"

>>> flightsPath = "/flights.csv"

>>> airportsPath = "/airports.csv"

>>> airlines = spark.read.format("csv").option("header","true").load(airlinesPath)

>>> airlines.createOrReplaceTempView("airlines_Table")

>>> airlines = spark.sql("select * from airlines_Table")
>>> airlines.columns
['Code', 'Description']

>>> airlines.printSchema()
root
 |-- Code: string (nullable = true)
 |-- Description: string (nullable = true)
 
>>> airlines.show(5)
+-----+--------------------+
| Code|         Description|
+-----+--------------------+
|19031|Mackey Internatio...|
|19032|Munz Northern Air...|
|19033|Cochise Airlines ...|
|19034|Golden Gate Airli...|
|19035|  Aeromech Inc.: RZZ|
+-----+--------------------+
only showing top 5 rows

>>> flights = spark.read.format("csv").option("header","true").load(flightsPath)

>>> flights.createOrReplaceTempView("flights_Table")

>>> flights.printSchema()
root
 |-- date: string (nullable = true)
 |-- airlines: string (nullable = true)
 |-- flight_number: string (nullable = true)
 |-- origin: string (nullable = true)
 |-- destination: string (nullable = true)
 |-- departure: string (nullable = true)
 |-- departure_delay: string (nullable = true)
 |-- arrival: string (nullable = true)
 |-- arrival_delay: string (nullable = true)
 |-- air_time: string (nullable = true)
 |-- distance: string (nullable = true)
 
>>> flights.show(5)
+--------+--------+-------------+------+-----------+---------+---------------+-------+-------------+--------+--------+
|    date|airlines|flight_number|origin|destination|departure|departure_delay|arrival|arrival_delay|air_time|distance|
+--------+--------+-------------+------+-----------+---------+---------------+-------+-------------+--------+--------+
|4/1/2014|   19805|            1|   JFK|        LAX|      854|             -6|   1217|            2|     355|    2475|
|4/1/2014|   19805|            2|   LAX|        JFK|      944|             14|   1736|          -29|     269|    2475|
|4/1/2014|   19805|            3|   JFK|        LAX|     1224|             -6|   1614|           39|     371|    2475|
|4/1/2014|   19805|            4|   LAX|        JFK|     1240|             25|   2028|          -27|     264|    2475|
|4/1/2014|   19805|            5|   DFW|        HNL|     1300|             -5|   1650|           15|     510|    3784|
+--------+--------+-------------+------+-----------+---------+---------------+-------+-------------+--------+--------+

>>> flights.count(),airlines.count()
(476881, 1579)  

>>> flights_count = spark.sql('select count(*) from flights_Table')
>>> airlines_count = spark.sql('select count(*) from airlines_Table')
>>> flights_count.show()
+--------+                                                                      
|count(1)|
+--------+
|  476881|
+--------+

>>> airlines_count.show()
+--------+
|count(1)|
+--------+
|    1579|
+--------+

>>> flights_count, airlines_count
(DataFrame[count(1): bigint], DataFrame[count(1): bigint])

>>> flights_count.collect()[0][0],airlines_count.collect()[0][0]
(476881, 1579)
    
>>> total_distance_df = spark.sql('select distance from flights_Table')\
.agg({"distance":"sum"})\
.withColumnRenamed("sum(distance)","Total_DIstance")

>>> total_distance_df.show()
+--------------+                                                                
|Total_DIstance|
+--------------+
|  2.38762505E8|
+--------------+

>>> all_delayed_2012 = spark.sql("select date, airlines, flight_number, departure_delay " 
+ "from flights_Table where departure_delay > 0 and year(date) = 2012")

>>> all_delayed_2012.show()
+----+--------+-------------+---------------+
|date|airlines|flight_number|departure_delay|
+----+--------+-------------+---------------+
+----+--------+-------------+---------------+

#>>> all_delayed_2014 = spark.sql("select date, airlines, flight_number, departure_delay " 
#+ "from flightsTable where departure_delay > 0 and year(date) = 2014")                           ------- year function is not working

>>> all_delayed_2014 = spark.sql("select date, airlines, flight_number, departure_delay from flights_Table where departure_delay > 0")

>>> all_delayed_2014.show()
+--------+--------+-------------+---------------+
|    date|airlines|flight_number|departure_delay|
+--------+--------+-------------+---------------+
|4/1/2014|   19805|            2|             14|
|4/1/2014|   19805|            4|             25|
|4/1/2014|   19805|            6|            126|
|4/1/2014|   19805|            7|            125|


>>> all_delayed_2014.orderBy(all_delayed_2014.departure_delay.desc()).show(3)
+--------+--------+-------------+---------------+                               
|    date|airlines|flight_number|departure_delay|
+--------+--------+-------------+---------------+
|4/1/2014|   19977|          712|             99|
|4/2/2014|   20304|         5239|             99|
|4/2/2014|   19805|          156|             99|
+--------+--------+-------------+---------------+
only showing top 3 rows

>>> all_delayed_2014.createOrReplaceTempView("all_delays")

>>> delay_count = spark.sql("select count(departure_delay) from all_delays")

>>> delay_count.show()
+----------------------+                                                        
|count(departure_delay)|
+----------------------+
|                114080|
+----------------------+

>>> delay_count.collect()[0][0]
114080

find out %???

continue-----
https://app.pluralsight.com/player?course=spark-2-getting-started&author=janani-ravi&name=9f634640-7cae-43f5-b952-e5c85d915cb5&clip=2&mode=live