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


>>> record_df.registerTempTable("records")

NOTE :- in SPARK 2.X it is 	df.createOrReplaceTempView("table1")

>>> all_record_df = sqlContext.sql("select * from records")

ERROR:- pyspark.sql.utils.AnalysisException: u'Table not found: records;'