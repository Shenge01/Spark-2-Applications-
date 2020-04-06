----------------------------------
 Janani Ravi
----------------------------------

>>> data = spark.read\
...             .format('csv')\
...             .option('header','true')\
...             .load('london_crime_by_lsoa_30k.csv')
18/08/19 09:27:58 WARN org.apache.spark.sql.execution.streaming.FileStreamSink: Error while looking for metadata directory.
18/08/19 09:27:58 WARN org.apache.spark.sql.execution.streaming.FileStreamSink: Error while looking for metadata directory.

>>> data.printSchema()
root
 |-- lsoa_code: string (nullable = true)
 |-- borough: string (nullable = true)
 |-- major_category: string (nullable = true)
 |-- minor_category: string (nullable = true)
 |-- value: string (nullable = true)
 |-- year: string (nullable = true)
 |-- month: string (nullable = true)

>>> data.show()
+---------+--------------------+--------------------+--------------------+-----+----+-----+
|lsoa_code|             borough|      major_category|      minor_category|value|year|month|
+---------+--------------------+--------------------+--------------------+-----+----+-----+
|E01001116|             Croydon|            Burglary|Burglary in Other...|    0|2016|   11|
|E01001646|           Greenwich|Violence Against ...|      Other violence|    0|2016|   11|
|E01000677|             Bromley|Violence Against ...|      Other violence|    0|2015|    5|
|E01003774|           Redbridge|            Burglary|Burglary in Other...|    0|2016|    3|
|E01004563|          Wandsworth|             Robbery|   Personal Property|    0|2008|    6|
|E01001320|              Ealing|  Theft and Handling|         Other Theft|    0|2012|    5|
|E01001342|              Ealing|Violence Against ...|    Offensive Weapon|    0|2010|    7|
|E01002633|            Hounslow|             Robbery|   Personal Property|    0|2013|    4|
|E01003496|              Newham|     Criminal Damage|Criminal Damage T...|    0|2013|    9|
|E01004177|              Sutton|  Theft and Handling|Theft/Taking of P...|    1|2016|    8|
|E01001985|            Haringey|  Theft and Handling|Motor Vehicle Int...|    0|2013|   12|
|E01003076|             Lambeth|Violence Against ...|      Other violence|    0|2015|    4|
|E01003852|Richmond upon Thames|             Robbery|   Personal Property|    0|2014|    1|
|E01004547|          Wandsworth|Violence Against ...|    Offensive Weapon|    0|2011|   10|
|E01002398|          Hillingdon|  Theft and Handling|Theft/Taking Of M...|    0|2016|    2|
|E01002358|            Havering|Violence Against ...|        Wounding/GBH|    0|2012|    2|
|E01000086|Barking and Dagenham|  Theft and Handling|  Other Theft Person|    1|2009|    5|
|E01003708|           Redbridge|Violence Against ...|      Common Assault|    0|2009|    6|
|E01002945|Kingston upon Thames|  Theft and Handling|    Theft From Shops|    0|2016|   11|
|E01004195|              Sutton|               Drugs| Possession Of Drugs|    0|2009|   10|
+---------+--------------------+--------------------+--------------------+-----+----+-----+
only showing top 20 rows

>>> data.count()
3098
>>> data.limit(5).show()
+---------+----------+--------------------+--------------------+-----+----+-----+
|lsoa_code|   borough|      major_category|      minor_category|value|year|month|
+---------+----------+--------------------+--------------------+-----+----+-----+
|E01001116|   Croydon|            Burglary|Burglary in Other...|    0|2016|   11|
|E01001646| Greenwich|Violence Against ...|      Other violence|    0|2016|   11|
|E01000677|   Bromley|Violence Against ...|      Other violence|    0|2015|    5|
|E01003774| Redbridge|            Burglary|Burglary in Other...|    0|2016|    3|
|E01004563|Wandsworth|             Robbery|   Personal Property|    0|2008|    6|
+---------+----------+--------------------+--------------------+-----+----+-----+

>>> data.show(5)
+---------+----------+--------------------+--------------------+-----+----+-----+
|lsoa_code|   borough|      major_category|      minor_category|value|year|month|
+---------+----------+--------------------+--------------------+-----+----+-----+
|E01001116|   Croydon|            Burglary|Burglary in Other...|    0|2016|   11|
|E01001646| Greenwich|Violence Against ...|      Other violence|    0|2016|   11|
|E01000677|   Bromley|Violence Against ...|      Other violence|    0|2015|    5|
|E01003774| Redbridge|            Burglary|Burglary in Other...|    0|2016|    3|
|E01004563|Wandsworth|             Robbery|   Personal Property|    0|2008|    6|
+---------+----------+--------------------+--------------------+-----+----+-----+
only showing top 5 rows

>>> data.count()
3098

==================Data cleaning : dropping rows having missing information
 
>>> data.dropna()
DataFrame[lsoa_code: string, borough: string, major_category: string, minor_category: string, value: string, year: string, month: string]

>>> data.count()
3098

>>> data.printSchema()
root
 |-- lsoa_code: string (nullable = true)
 |-- borough: string (nullable = true)
 |-- major_category: string (nullable = true)
 |-- minor_category: string (nullable = true)
 |-- value: string (nullable = true)
 |-- year: string (nullable = true)
 |-- month: string (nullable = true)

================= Dropping column which is not required :---- using drop()
  
>>> data = data.drop('lsoa_code')

>>> data.show(5)
+----------+--------------------+--------------------+-----+----+-----+
|   borough|      major_category|      minor_category|value|year|month|
+----------+--------------------+--------------------+-----+----+-----+
|   Croydon|            Burglary|Burglary in Other...|    0|2016|   11|
| Greenwich|Violence Against ...|      Other violence|    0|2016|   11|
|   Bromley|Violence Against ...|      Other violence|    0|2015|    5|
| Redbridge|            Burglary|Burglary in Other...|    0|2016|    3|
|Wandsworth|             Robbery|   Personal Property|    0|2008|    6|
+----------+--------------------+--------------------+-----+----+-----+
only showing top 5 rows

=========================Unique values in dataset :- distinct()

>>> total_borough = data.select('borough').distinct()

>>> total_borough.show()
+--------------------+                                                          
|             borough|
+--------------------+
|             Croydon|
|          Wandsworth|
|              Bexley|
|             Lambeth|
|Barking and Dagenham|
|              Camden|
|           Greenwich|

>>> total_borough.count()
33                                                                              

==================Filtering data :-    filter()

>>> hackney_data = data.filter(data['borough'] == 'Hackney')

>>> hackney_data.show(5)
+-------+--------------------+--------------------+-----+----+-----+
|borough|      major_category|      minor_category|value|year|month|
+-------+--------------------+--------------------+-----+----+-----+
|Hackney|     Criminal Damage|Criminal Damage T...|    0|2011|    6|
|Hackney|Violence Against ...|          Harassment|    1|2013|    2|
|Hackney|     Criminal Damage|Other Criminal Da...|    0|2011|    7|
|Hackney|Violence Against ...|        Wounding/GBH|    0|2013|   12|
|Hackney|  Theft and Handling|  Other Theft Person|    0|2016|    8|
+-------+--------------------+--------------------+-----+----+-----+
only showing top 5 rows

===============Filtering use isin() to filter out data

>>> data_2015_2016 = data.filter(data['year'].isin(['2015','2016']))

>>> data_2015_2016.show(5)
+---------+--------------------+--------------------+-----+----+-----+
|  borough|      major_category|      minor_category|value|year|month|
+---------+--------------------+--------------------+-----+----+-----+
|  Croydon|            Burglary|Burglary in Other...|    0|2016|   11|
|Greenwich|Violence Against ...|      Other violence|    0|2016|   11|
|  Bromley|Violence Against ...|      Other violence|    0|2015|    5|
|Redbridge|            Burglary|Burglary in Other...|    0|2016|    3|
|   Sutton|  Theft and Handling|Theft/Taking of P...|    1|2016|    8|
+---------+--------------------+--------------------+-----+----+-----+
only showing top 5 rows

==========================taking Sample :- sample of fraction (0.1)

>>> data_2015_2016.sample(fraction=0.1).show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: sample() takes at least 3 arguments (2 given)
