
**********************
Janani Ravi 
**********************
--------------------------------
Create Data Frame from text file
--------------------------------

>>> DataText = sc.textFile("london_crime_by_lsoa_30k.txt")

>>> DataText_MAP = DataText.map(lambda i: (str(i.split(",")[0]),str(i.split(",")[1]),str(i.split(",")[2]),str(i.split(",")[3]),int(i.split(",")[4]),int(i.split(",")[5]),int(i.split(",")[6])))

>>> toDataFrame = DataText_MAP.toDF(schema=["lsoa_code","borough","major_category","minor_category","value","year","month"])

>>> toDataFrame.limit(5).show()

+---------+----------+--------------------+--------------------+-----+----+-----+
|lsoa_code|   borough|      major_category|      minor_category|value|year|month|
+---------+----------+--------------------+--------------------+-----+----+-----+
|E01001116|   Croydon|            Burglary|Burglary in Other...|    0|2016|   11|
|E01001646| Greenwich|Violence Against ...|      Other violence|    0|2016|   11|
|E01000677|   Bromley|Violence Against ...|      Other violence|    0|2015|    5|
|E01003774| Redbridge|            Burglary|Burglary in Other...|    0|2016|    3|
|E01004563|Wandsworth|             Robbery|   Personal Property|    0|2008|    6|
+---------+----------+--------------------+--------------------+-----+----+-----+

>>> data = toDataFrame.drop("lsoa_code")
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

>>> total_borough = data.select("borough").distinct()

>>> total_borough.show()


+--------------------+
|             borough|
+--------------------+
|              Merton|
|Richmond upon Thames|
|            Hounslow|
|           Southwark|
|             Hackney|
|              Sutton|
|          Hillingdon|
|            Haringey|

>>> total_borough.count()

33

>>> hackney_data = data.filter(data['borough'] == "Hackney")

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

>>> data_2015_2016 = data.filter(data['year'].isin(["2015","2016"]))

>>> data_2015_2016.show()

+--------------------+--------------------+--------------------+-----+----+-----+
|             borough|      major_category|      minor_category|value|year|month|
+--------------------+--------------------+--------------------+-----+----+-----+
|             Croydon|            Burglary|Burglary in Other...|    0|2016|   11|
|           Greenwich|Violence Against ...|      Other violence|    0|2016|   11|
|             Bromley|Violence Against ...|      Other violence|    0|2015|    5|
|           Redbridge|            Burglary|Burglary in Other...|    0|2016|    3|
|              Sutton|  Theft and Handling|Theft/Taking of P...|    1|2016|    8|
|             Lambeth|Violence Against ...|      Other violence|    0|2015|    4|
|          Hillingdon|  Theft and Handling|Theft/Taking Of M...|    0|2016|    2|
|Kingston upon Thames|  Theft and Handling|    Theft From Shops|    0|2016|   11|
|            Haringey|Violence Against ...|        Wounding/GBH|    0|2015|   12|
|            Lewisham|Violence Against ...|      Common Assault|    0|2016|    2|
|            Hounslow|     Criminal Damage|Criminal Damage T...|    0|2015|    2|
|             Bromley|     Criminal Damage|Criminal Damage T...|    1|2016|    4|
|            Haringey|     Criminal Damage|Criminal Damage T...|    0|2016|   12|
|           Southwark|               Drugs| Possession Of Drugs|    0|2015|    3|
|            Havering|    Fraud or Forgery|  Counted per Victim|    0|2015|   11|
|      Waltham Forest|Other Notifiable ...|      Going Equipped|    0|2015|    2|
|              Ealing|             Robbery|   Personal Property|    0|2015|    7|
|               Brent|  Theft and Handling|Motor Vehicle Int...|    0|2015|    9|
|            Hounslow|Violence Against ...|        Wounding/GBH|    2|2015|    8|
|           Southwark|  Theft and Handling|    Theft From Shops|    4|2016|    8|
+--------------------+--------------------+--------------------+-----+----+-----+

>>> data_2015_2016 = data.filter(data['year'] >= 2014)

>>> borough_crime_count = data.groupBy('borough').count()

>>> borough_crime_count.show()

+--------------------+-----+
|             borough|count|
+--------------------+-----+
|              Merton|   74|
|Richmond upon Thames|   84|
|            Hounslow|   86|
|           Southwark|  123|
|             Hackney|   95|
|              Sutton|   76|
|          Hillingdon|  101|
|            Haringey|   99|

>>> borough_conviction_sum = data.groupBy("borough").agg({"value":"sum"})
>>> borough_conviction_sum.show()

+--------------------+----------+
|             borough|sum(value)|
+--------------------+----------+
|              Merton|        24|
|Richmond upon Thames|        22|
|            Hounslow|        43|
|           Southwark|        65|
|             Hackney|        84|
|              Sutton|        32|

>>> borough_conviction_sum = data.groupBy("borough").agg({"value":"sum"}).withColumnRenamed("sum(value)","convictions")

>>> borough_conviction_sum.show()


+--------------------+----------+
|             borough|convictions|
+--------------------+----------+
|              Merton|        24|
|Richmond upon Thames|        22|
|            Hounslow|        43|
|           Southwark|        65|
|             Hackney|        84|
+--------------------+----------+
only showing top 5 rows

>>> total_borough_convictions = borough_conviction_sum.agg({"convictions":"sum"})
>>> total_borough_convictions.show()

+----------------+
|sum(convictions)|
+----------------+
|            1552|
+----------------+

>>> total_convictions = total_borough_convictions.collect()[0][0]

NOTE:- Using collect() find out perticulat record and stor it in total_convictions

>>> import pyspark.sql.functions as func

>>> borough_percentage_contribution = borough_conviction_sum.withColumn("Contribution %", func.round(borough_conviction_sum.convictions / total_convictions *100,2))

>>> borough_percentage_contribution.printSchema()
root
 |-- borough: string (nullable = true)
 |-- convictions: long (nullable = true)
 |-- Contribution %: double (nullable = true)
 
>>> borough_percentage_contribution.orderBy(borough_percentage_contribution[2].desc()).show(10)

+-----------+-----------+--------------+
|    borough|convictions|Contribution %|
+-----------+-----------+--------------+
| Hillingdon|        102|          6.57|
|Westminster|         86|          5.54|
|    Hackney|         84|          5.41|
|    Lambeth|         83|          5.35|
|     Camden|         78|          5.03|
|    Croydon|         72|          4.64|
|  Southwark|         65|          4.19|
| Wandsworth|         65|          4.19|
|   Haringey|         56|          3.61|
|     Ealing|         55|          3.54|
+-----------+-----------+--------------+
only showing top 10 rows

>>> crimes_category = data.groupBy("major_category").agg({"value":"sum"}).withColumnRenamed("sum(value)","convictions")

>>> crimes_category.orderBy(crimes_category.convictions.desc()).show()

+--------------------+-----------+
|      major_category|convictions|
+--------------------+-----------+
|  Theft and Handling|        664|
|Violence Against ...|        379|
|            Burglary|        193|
|     Criminal Damage|        128|
|               Drugs|        107|
|             Robbery|         51|
|Other Notifiable ...|         30|
|    Fraud or Forgery|          0|
|     Sexual Offences|          0|
+--------------------+-----------+

>>> year_df = data.select("year")

>>> year_df.agg({"year":"min"}).show()

+---------+
|min(year)|
+---------+
|     2008|
+---------+

>>> year_df.agg({"year":"max"}).show()

+---------+
|max(year)|
+---------+
|     2016|
+---------+

>>> year_df.describe().show()

+-------+------------------+
|summary|              year|
+-------+------------------+
|  count|              3098|
|   mean|2011.9351194318915|
| stddev| 2.596659574284582|
|    min|              2008|
|    max|              2016|
+-------+------------------+

>>> data.crosstab('borough','major_category').select('borough_major_category','Burglary','Drugs','Robbery').show()

+----------------------+--------+-----+-------+
|borough_major_category|Burglary|Drugs|Robbery|
+----------------------+--------+-----+-------+
|              Havering|       6|    7|      8|
|                Merton|       5|    8|      5|
|              Haringey|       7|    3|      4|
|         Tower Hamlets|       6|    5|      5|
|               Bromley|       5|    6|      5|


Continue ---  name 'get_ipython' is not defined
https://app.pluralsight.com/player?course=spark-2-getting-started&author=janani-ravi&name=3a4fd582-8dad-4543-b79e-defc5ca53860&clip=4&mode=live

>>> get_ipython().magic('matplotlib inline')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'get_ipython' is not defined
