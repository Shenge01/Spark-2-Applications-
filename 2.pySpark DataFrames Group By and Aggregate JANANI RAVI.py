
**************************groupBy & Aggrigate in pySpark DataFrames************************


>>> df = sqlContext.read.json("stocks.json")
    
>>> df.printSchema()

Performance (Month)
    
>>> df.select('20-Day Simple Moving Average','P/S','Gap','Earnings Date','Gross Margin','Performance (Year)','Shares Float').show()
    
>>> df_new = df.select('Company','Country','20-Day Simple Moving Average','P/S','Gap','Earnings Date','Gross Margin','Performance (Year)','Shares Float')

>>> df_new_1 = df.select('Company','Country','P/S','Earnings Date','Gross Margin')

>>> df_new_1
DataFrame[Company: string, Country: string, P/S: double, Earnings Date: struct<$date:bigint>, Gross Margin: double]

>>> type(df_new_1)
<class 'pyspark.sql.dataframe.DataFrame'>

>>> df_new_1.printSchema()
root
 |-- Company: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- P/S: double (nullable = true)
 |-- Earnings Date: struct (nullable = true)
 |    |-- $date: long (nullable = true)
 |-- Gross Margin: double (nullable = true)

>>> df_new_1.groupBy('Country').mean().show()

+--------------------+------------------+-------------------+
|             Country|          avg(P/S)|  avg(Gross Margin)|
+--------------------+------------------+-------------------+
|          Luxembourg|1.9828571428571429|0.49642857142857144|
|              Cyprus|              2.65|              0.441|
|              Mexico|          2.536875| 0.3826428571428571|
|             Denmark|              6.33|              0.831|
|               India| 3.313333333333333|             0.3908|
|            Portugal|              0.44|              0.574|

>>> df_new_1.groupBy('Country').max().show()

+--------------------+---------+-----------------+
|             Country| max(P/S)|max(Gross Margin)|
+--------------------+---------+-----------------+
|          Luxembourg|     4.84|            0.941|
|              Cyprus|     2.65|            0.441|
|              Mexico|    12.07|            0.698|
|             Denmark|     6.33|            0.831|
|               India|     6.94|            0.534|

===================> groupBy can only be used by pair RDD and you can only group by the key of pair.

>>> df_new_1.groupBy('Country').min().show()

>>> df_new_1.groupBy('Country').count().show()

+--------------------+-----+
|             Country|count|
+--------------------+-----+
|          Luxembourg|    7|
|              Cyprus|    1|
|              Mexico|   17|
|             Denmark|    1|
|               India|   12|
|            Portugal|    1|

========================> Performing aggregation  ==> sum :-

>>> df_new_1.agg({'Gross Margin':'sum'}).show()

+------------------+
| sum(Gross Margin)|
+------------------+
|1560.1978000000008|
+------------------+

>>> df_new_1.agg({'Gross Margin':'max'}).show()

+-----------------+
|max(Gross Margin)|
+-----------------+
|              1.0|
+-----------------+

>>> group_date = df.groupBy('Country')

>>> group_date.agg({'Gross Margin':'max'}).show()

+--------------------+-----------------+
|             Country|max(Gross Margin)|
+--------------------+-----------------+
|          Luxembourg|            0.941|
|              Cyprus|            0.441|
|              Mexico|            0.698|
|             Denmark|            0.831|
|               India|            0.534|

====================> Renaming coulmn using - withColumnRenamed("<old_col_name>","new_col_name")

>>> max_Gross_margin_by_countries = df.groupBy('Country')\
							.agg({'Gross Margin':'max'})\
							.withColumnRenamed("max(Gross Margin)","Maximum_By_Country")

===================> We can perform sum aggregation without a groupby on renamed column

>>> SUM_max_Gross_margin_by_countries = max_Gross_margin_by_countries.agg("Maximum_By_Country":"sum")	


	
>>> from pyspark.sql.functions import *

>>> df.select(avg('Gross Margin')).show()

+------------------+
| avg(Gross Margin)|
+------------------+
|0.4298065564738294|
+------------------+

>>> distinct_Countries = df_new_1.select('Country').distinct()
>>> distinct_Countries.show()

>>> distinct_Countries.count()
47

>>> Only_USA_Data = df_new_1.filter(df_new_1['Country'] == 'USA' )
>>> Only_USA_Data.show()


+--------------------+-------+-----+---------------+------------+
|             Company|Country|  P/S|  Earnings Date|Gross Margin|
+--------------------+-------+-----+---------------+------------+
|Agilent Technolog...|    USA| 2.54|[1384464600000]|       0.512|
|         Alcoa, Inc.|    USA| 0.41|[1381264200000]|       0.163|
|WCM/BNY Mellon Fo...|    USA| null|           null|        null|
|iShares MSCI AC A...|    USA| null|           null|        null|
|Altisource Asset ...|    USA| null|           null|        null|
|Atlantic American...|    USA| 0.57|[1383541200000]|        null|

>>> data_usa_canada = df_new_1.filter(df_new_1['Country'].isin(["USA","Canada"]))
>>> data_usa_canada.show()

+--------------------+-------+------+---------------+------------+
|             Company|Country|   P/S|  Earnings Date|Gross Margin|
+--------------------+-------+------+---------------+------------+
|Agilent Technolog...|    USA|  2.54|[1384464600000]|       0.512|
|         Alcoa, Inc.|    USA|  0.41|[1381264200000]|       0.163|
|WCM/BNY Mellon Fo...|    USA|  null|           null|        null|
|Almaden Minerals ...| Canada|250.21|           null|        null|
|Advantage Oil & G...| Canada|  2.64|[1300248000000]|       0.682|

---------------------------
Other Filter operations:-

data_2014_onwards = data.filter(data['year'] >= 2014 )

data_2014_onwards.sample(fraction=0.1).show()

---------------------------

>>> df_new_usa_sum = df_new.groupBy('Country').agg({"Gross Margin":"sum"})

>>> df_new_usa_sum.show(5)

+----------+-----------------+
|   Country|sum(Gross Margin)|
+----------+-----------------+
|Luxembourg|            3.475|
|    Cyprus|            0.441|
|    Mexico|5.356999999999999|
|   Denmark|            0.831|
|     India|           3.5172|
+----------+-----------------+

>>> df_new_usa_sum = df_new.groupBy('Country')\
			   .agg({"Gross Margin":"sum"})\
			   .withColumnRenamed("sum(Gross Margin)","Country_Gross_Margin")
>>> df_new_usa_sum.show()

+--------------------+--------------------+
|             Country|Country_Gross_Margin|
+--------------------+--------------------+
|          Luxembourg|               3.475|
|              Cyprus|               0.441|
|              Mexico|   5.356999999999999|

NOTE:- withColumnRenamed() -->


>>> TOTAL_Country_Gross_Margin = df_new_usa_sum.agg({"Country_Gross_Margin":"sum"})
>>> TOTAL_Country_Gross_Margin.show()

+-------------------------+
|sum(Country_Gross_Margin)|
+-------------------------+
|       1560.1977999999986|
+-------------------------+

=================================== Access the value of cell by calling collect and store result :-

>>> TOTAL_All_Country_Gross_Margin = TOTAL_Country_Gross_Margin.collect()[0][0]

>>> import pyspark.sql.functions as func

>>> Percentage_Contributioin = df_new_usa_sum\
		.withColumn("Percentage %", func.round(df_new_usa_sum.Country_Gross_Margin / TOTAL_All_Country_Gross_Margin * 100,2))


NOTE:- withColumn() -->
	
=================================printSchema() :- to show schema of dataframe

>>> Percentage_Contributioin.printSchema()
root
 |-- Country: string (nullable = true)
 |-- Country_Gross_Margin: double (nullable = true)
 |-- Percentage %: double (nullable = true)
 
>>> Percentage_Contributioin.show()

+--------------------+--------------------+------------+
|             Country|Country_Gross_Margin|Percentage %|
+--------------------+--------------------+------------+
|          Luxembourg|               3.475|        0.22|
|              Cyprus|               0.441|        0.03|
|              Mexico|   5.356999999999999|        0.34|
|             Denmark|               0.831|        0.05|
|               India|              3.5172|        0.23|
|            Portugal|               0.574|        0.04|

>>> Percentage_Contributioin.orderBy(Percentage_Contributioin[2].desc()).show()

+--------------+--------------------+------------+
|       Country|Country_Gross_Margin|Percentage %|
+--------------+--------------------+------------+
|           USA|  1307.0260999999991|       83.77|
|         China|   55.73289999999999|        3.57|
|        Canada|             39.7471|        2.55|
|        Israel|  26.206700000000005|        1.68|
|United Kingdom|  13.591000000000001|        0.87|
|        Greece|              12.224|        0.78|
|       Bermuda|  12.197999999999999|        0.78|
|   Netherlands|               9.214|        0.59|
|     Hong Kong|              7.2614|        0.47|

NOTE:- orderby on the basis of 3rd column that is index 2.
		You can check that Country_Gross_Margin 1307 is 83.77% of sum(Country_Gross_Margin). 

For More check link:-
https://app.pluralsight.com/player?course=spark-2-getting-started&author=janani-ravi&name=3a4fd582-8dad-4543-b79e-defc5ca53860&clip=3&mode=live



-----------------------------------------------------------------------------------
>>> df.select(count('Country')).show()

+--------------+
|count(Country)|
+--------------+
|          6756|
+--------------+

>>> df.select(avg('Gross Margin').alias('AVG GROSS SAL')).show()

+------------------+
|     AVG GROSS SAL|
+------------------+
|0.4298065564738294|
+------------------+

Format Output:---

>>> df_format = df.select(avg('Gross Margin').alias('AVG_GROSS_SAL'))

>>> df_format.select(format_number('AVG_GROSS_SAL',2)).show()

+------------------------------+
|format_number(AVG_GROSS_SAL,2)|
+------------------------------+
|                          0.43|
+------------------------------+


-------------NOTE-------------

>>> a = df_new_1.groupBy('Country')

>>> type(a)
<class 'pyspark.sql.group.GroupedData'>

>>> a.show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
AttributeError: 'GroupedData' object has no attribute 'show'

==> You can not perform show() on GroupedData

>>> b= df_new_1.groupBy('Country').mean()
>>> b
DataFrame[Country: string, avg(P/S): double, avg(Gross Margin): double]
>>> type(b)
<class 'pyspark.sql.dataframe.DataFrame'>

==> Now you can can perform show()

--------------------------------
