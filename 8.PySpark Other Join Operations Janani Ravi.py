-------------------------------------
Other Join Operations:- Janani Ravi:-
-------------------------------------

>>> valuesA = [('John',100000), ('James',150000),('Emily',65000),('Nina',200000)]

>>> from pyspark.sql.types import *
>>> from pyspark.sql.functions import *
>>> from pyspark.sql import Row
>>> tableA = sqlContext.createDataFrame(valuesA,['Name','Salary'])

+-----+------+
| Name|Salary|
+-----+------+
| John|100000|
|James|150000|
|Emily| 65000|
| Nina|200000|
+-----+------+


>>> valuesB = [('John',1), ('James',2),('Darth',6),('Princess',5)]

>>> tableB = sqlContext.createDataFrame(valuesB,['Name','Dept'])

+--------+----+
|    Name|Dept|
+--------+----+
|    John|   1|
|   James|   2|
|   Darth|   6|
|Princess|   5|
+--------+----+

--------------
Inner JOIN
--------------

Contails only those records which contains in both tables.

>>> inner_join = tableA.join(tableB, tableA.Name == tableB.Name)
>>> inner_join.show()

+-----+------+-----+----+
| Name|Salary| Name|Dept|
+-----+------+-----+----+
| John|100000| John|   1|
|James|150000|James|   2|
+-----+------+-----+----+

---------------
LEFT Outer Join
---------------

left_join = tableA.join(tableB , tableA.Name == tableB.Name,how='left')
left_join.show()

+-----+------+-----+----+
| Name|Salary| Name|Dept|
+-----+------+-----+----+
| Nina|200000| null|null|
| John|100000| John|   1|
|Emily| 65000| null|null|
|James|150000|James|   2|
+-----+------+-----+----+


---------------
Right Outer Join
---------------

left_join = tableA.join(tableB , tableA.Name  == tableB.Name ,how='right')
left_join.show()

+-----+------+--------+----+
| Name|Salary|    Name|Dept|
+-----+------+--------+----+
| John|100000|    John|   1|
| null|  null|   Darth|   6|
| null|  null|Princess|   5|
|James|150000|   James|   2|
+-----+------+--------+----+
