----------------------------------
Querying Data Using Spark SQL : Demo: Inferred and Explicit Schemas :- Janani Ravi
----------------------------------

>>> from pyspark.sql import SparkSession
>>> from pyspark.sql.types import Row

>>> lines = sc.textFile("/students.txt")

>>> type(lines)
<class 'pyspark.rdd.RDD'>

>>> lines.collect()
[u'Emily,44,55,78', u'Andy,47,34,89', u'Rick,55,78,55', u'Aaron,66,34,98']  

NOTE:- in the lines RDD every record is string and it contains comma seperated values.
    
>>> parts = lines.map(lambda i: i.split(","))

>>> parts.collect()
[[u'Emily', u'44', u'55', u'78'], [u'Andy', u'47', u'34', u'89'], [u'Rick', u'55', u'78', u'55'], [u'Aaron', u'66', u'34', u'98']]

NOTE:- Now it is in list .

==> Convert every element in parts RDD to a Row object.

>>> students = parts.map(lambda i: Row(name=i[0], math=int(i[1]), english=int(i[2]), science=int(i[3])))
>>> students.collect()
[Row(english=55, math=44, name=u'Emily', science=78), Row(english=34, math=47, name=u'Andy', science=89), Row(english=78, math=55, name=u'Rick', science=55), Row(english=34, math=66, name=u'Aaron', science=98)]

--------> unable to create df using schemaStudents = spark.createDataFrmae(students)

>>> schemaStudents = students.toDF()

>>> schemaStudents.createOrReplaceTempView("stud_table")

>>> schemaStudents.columns
['english', 'math', 'name', 'science']

>>> schemaStudents.schema
StructType(List(StructField(english,LongType,true),StructField(math,LongType,true),StructField(name,StringType,true),StructField(science,LongType,true))


NOTE:- You can see , Spark is smart enough to infer Schema on the basis of values in each columns.

>>> spark.sql('select * from stud_table').show()
+-------+----+-----+-------+
|english|math| name|science|
+-------+----+-----+-------+
|     55|  44|Emily|     78|
|     34|  47| Andy|     89|
|     78|  55| Rick|     55|
|     34|  66|Aaron|     98|
+-------+----+-----+-------+

******************************************************

USE parts RDD to define Schema Explectly:-

>>> parts.collect()
[[u'Emily', u'44', u'55', u'78'], [u'Andy', u'47', u'34', u'89'], [u'Rick', u'55', u'78', u'55'], [u'Aaron', u'66', u'34', u'98']]

>>> from pyspark.sql.types import StructType, StructField, StringType, LongType

>>> fileds = (StructField('name',StringType(),True),
StructField('math',StringType(),True),
StructField('english',StringType(),True),
StructField('science',StringType(),True),
)

>>> schema = StructType(fileds)

>>> schemaStudents.columns
['name', 'math', 'english', 'science']

>>> schemaStudents.schema
StructType(List(StructField(name,StringType,true),StructField(math,StringType,true),StructField(english,StringType,true),StructField(science,StringType,true)))