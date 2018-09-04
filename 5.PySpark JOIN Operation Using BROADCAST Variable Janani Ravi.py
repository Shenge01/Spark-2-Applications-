**************************************************************
JOIN Operation in pyspark Using BROADCAST Variable Janani Ravi
**************************************************************

Use Script <PySpark Script to perform Join operation on player data> before below given code.

>>> players.printSchema()
root
 |-- player_api_id: string (nullable = true)
 |-- player_name: string (nullable = true)
 |-- birthday: string (nullable = true)
 |-- height: string (nullable = true)
 |-- weight: string (nullable = true)

>>> strikers.printSchema()
root
 |-- player_api_id: string (nullable = true)
 |-- FINISHING: double (nullable = true)
 |-- striker_grade: double (nullable = true)


>>> players.count(),strikers.count()

(11060, 1609)

NOTE:- strikers has less data , So we can use this as BROADCAST.

>>> from pyspark.sql.functions import broadcast

>>> striker_details = players.select("player_api_id","player_name").join(broadcast(strikers),["player_api_id"],"inner")

+-------------+-----------------+---------+-------------+
|player_api_id|      player_name|FINISHING|striker_grade|
+-------------+-----------------+---------+-------------+
|        27316|       Aaron Hunt|     72.0|        74.75|
|        40719|     Aaron Niguez|     67.0|        74.25|
|        75489|     Aaron Ramsey|     75.0|       76.875|
|       120919|Aatif Chahechouhe|     76.0|         78.0|
|        67334|Abdoul Karim Yoda|     70.0|         74.0|
+-------------+-----------------+---------+-------------+

>>> striker_details = striker_details.sort(striker_details.striker_grade.desc())

>>> striker_details.show(5)

+-------------+--------------+---------+-------------+
|player_api_id|   player_name|FINISHING|striker_grade|
+-------------+--------------+---------+-------------+
|        20276|          Hulk|     85.0|        89.25|
|        37412| Sergio Aguero|     90.0|         89.0|
|        38817|  Carlos Tevez|     88.0|        88.75|
|        32118|Lukas Podolski|     85.0|        88.25|
|        31921|   Gareth Bale|     81.0|         87.0|
+-------------+--------------+---------+-------------+
