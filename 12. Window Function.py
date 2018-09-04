----------------------------------
:- Janani Ravi
----------------------------------

Window :- Range of rows are called window.

>>> from pyspark.sql import SparkSession

>>> products = spark.read.format("csv").option('header','true').load('/products.csv')

>>> products.show()
+----------+--------+-----+
|   product|category|price|
+----------+--------+-----+
|Samsung TX|  Tablet|  999|
|Samsung JX|  Mobile|  799|
|Redmi Note|  Mobile|  399|
|        Mi|  Mobile|  299|
|      iPad|  Tablet|  789|
|    iPhone|  Mobile|  999|
|  Micromax|  Mobile|  249|
|    Lenovo|  Tablet|  499|
|   OnePlus|  Mobile|  356|
|        Xu|  Tablet|  267|
+----------+--------+-----+

>>> import sys
>>> from pyspark.sql.window import Window
>>> import pyspark.sql.functions as func

>>> windowSpec1 = Window.partitionBy(products['category'])\
...                     .orderBy(products['price'].desc())

>>> type(windowSpec1)
<class 'pyspark.sql.window.WindowSpec'>

>>> price_rank = (func.rank().over(windowSpec1))

>>> type(price_rank)
<class 'pyspark.sql.column.Column'>

>>> product_rank = products.select(products['product'],\
products['category'],\
products['price'])\
.withColumn('rank',func.rank().over(windowSpec1))

>>> product_rank.show()
+----------+--------+-----+----+                                                
|   product|category|price|rank|
+----------+--------+-----+----+
|    iPhone|  Mobile|  999|   1|
|Samsung JX|  Mobile|  799|   2|
|Redmi Note|  Mobile|  399|   3|
|   OnePlus|  Mobile|  356|   4|
|        Mi|  Mobile|  299|   5|
|  Micromax|  Mobile|  249|   6|
|Samsung TX|  Tablet|  999|   1|
|      iPad|  Tablet|  789|   2|
|    Lenovo|  Tablet|  499|   3|
|        Xu|  Tablet|  267|   4|
+----------+--------+-----+----+

Change window Specification:--

>>> windowSpec2 = Window.partitionBy(products['category'])\
.orderBy(products['price'].desc())\
.rowsBetween(-1,0)

NOTE:- 0 is current row and -1 is previous row.

>>> price_max = (func.max(products['price']).over(windowSpec2))

>>> products.select(products['product'],\
products['category'],\
products['price'],\
price_max.alias("price_max")).show()

+----------+--------+-----+---------+                                           
|   product|category|price|price_max|
+----------+--------+-----+---------+
|    iPhone|  Mobile|  999|      999|
|Samsung JX|  Mobile|  799|      999|
|Redmi Note|  Mobile|  399|      799|
|   OnePlus|  Mobile|  356|      399|
|        Mi|  Mobile|  299|      356|
|  Micromax|  Mobile|  249|      299|
|Samsung TX|  Tablet|  999|      999|
|      iPad|  Tablet|  789|      999|
|    Lenovo|  Tablet|  499|      789|
|        Xu|  Tablet|  267|      499|
+----------+--------+-----+---------+

Another window Specification:-

>>> windowSpec3 = Window.partitionBy(products['category'])\
.orderBy(products['price'].desc())\
.rangeBetween(-sys.maxsize, sys.maxsize)

NOTE:- This will apply on the column in which we have done ordering.

>>> price_difference = (func.max(products['price']).over(windowSpec3) - product['price'])

>>> products.select(products['product'],\
products['category'],\
products['price'],\
price_difference.alias("price_difference")).show()

+----------+--------+-----+----------------+                                    
|   product|category|price|price_difference|
+----------+--------+-----+----------------+
|    iPhone|  Mobile|  999|             0.0|
|Samsung JX|  Mobile|  799|           200.0|
|Redmi Note|  Mobile|  399|           600.0|
|   OnePlus|  Mobile|  356|           643.0|
|        Mi|  Mobile|  299|           700.0|
|  Micromax|  Mobile|  249|           750.0|
|Samsung TX|  Tablet|  999|             0.0|
|      iPad|  Tablet|  789|           210.0|
|    Lenovo|  Tablet|  499|           500.0|
|        Xu|  Tablet|  267|           732.0|
+----------+--------+-----+----------------+

