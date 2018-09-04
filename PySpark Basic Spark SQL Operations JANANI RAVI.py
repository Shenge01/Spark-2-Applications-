----------------------------------
Querying Data Using Spark SQL : Demo: Basic Spark SQL Operations:- Janani Ravi
----------------------------------

>>> from pyspark.sql.types import Row
from datetime import datetime

##### Creating a dataframe with different data types

record = sc.parallelize([Row(id = 1,
                             name = "Jill",
                             active = True,
                             clubs = ['chess', 'hockey'],
                             subjects = {"math": 80, 'english': 56},
                             enrolled = datetime(2014, 8, 1, 14, 1, 5)),
                         Row(id = 2,
                             name = "George",
                             active = False,
                             clubs = ['chess', 'soccer'],
                             subjects = {"math": 60, 'english': 96},
                             enrolled = datetime(2015, 3, 21, 8, 2, 5))
])

>>> record_df = record.toDF()

>>> record_df.show()

+------+---------------+--------------------+---+------+--------------------+
|active|          clubs|            enrolled| id|  name|            subjects|
+------+---------------+--------------------+---+------+--------------------+
|  true|[chess, hockey]|2014-08-01 14:01:...|  1|  Jill|Map(math -> 80, e...|
| false|[chess, soccer]|2015-03-21 08:02:...|  2|George|Map(math -> 60, e...|
+------+---------------+--------------------+---+------+--------------------+


NOTE:- 	If you want to run SQL query on this data then need to register DataFrame as a Table.
		Table will be available only for a session.
		Not shares across spark sessions.


#>>> record_df.registerTempTable("records") ----- in Spark 1.6

>>> record_df.createOrReplaceTempView("table1") -- table for session only

# table1 is the name of our sql table.

>>> all_records_df = sqlContext.sql('select * from table1')

# This sql query will act as data frame now , we can perform all data frame operations

>>> all_records_df.show()
+------+---------------+-------------------+---+------+--------------------+
|active|          clubs|           enrolled| id|  name|            subjects|
+------+---------------+-------------------+---+------+--------------------+
|  true|[chess, hockey]|2014-08-01 14:01:05|  1|  Jill|Map(english -> 56...|
| false|[chess, soccer]|2015-03-21 08:02:05|  2|George|Map(english -> 96...|
+------+---------------+-------------------+---+------+--------------------+

NOTE:- Complex sql query from table table1:-

>>> sqlContext.sql('select id,clubs[1], subjects["english"] from table1').show()
+---+--------+-----------------+
| id|clubs[1]|subjects[english]|
+---+--------+-----------------+
|  1|  hockey|               56|
|  2|  soccer|               96|
+---+--------+-----------------+

>>> sqlContext.sql('select * from table1 where subjects["english"] > 90').show()
+------+---------------+-------------------+---+------+--------------------+
|active|          clubs|           enrolled| id|  name|            subjects|
+------+---------------+-------------------+---+------+--------------------+
| false|[chess, soccer]|2015-03-21 08:02:05|  2|George|Map(english -> 96...|
+------+---------------+-------------------+---+------+--------------------+



*******************************GLOBAL TABLE******************************

Register a table so that it can access accross the session.

>>> record_df.createGlobalTempView("global_records")

>>> sqlContext.sql('select * from global_temp.global_records').show()
+------+---------------+-------------------+---+------+--------------------+
|active|          clubs|           enrolled| id|  name|            subjects|
+------+---------------+-------------------+---+------+--------------------+
|  true|[chess, hockey]|2014-08-01 14:01:05|  1|  Jill|Map(english -> 56...|
| false|[chess, soccer]|2015-03-21 08:02:05|  2|George|Map(english -> 96...|
+------+---------------+-------------------+---+------+--------------------+

