*************************************************************************************
Working with Accumulators --> Height of a player results in a better heading accuracy:- Janani Ravi
*************************************************************************************

Use Script <PySpark Script to perform Join operation on player data> before below given code.


>>> players.count(), player_attributes.count()

(11060, 183978)

>>> players.printSchema(), player_attributes.printSchema()
root
 |-- player_api_id: string (nullable = true)
 |-- player_name: string (nullable = true)
 |-- birthday: string (nullable = true)
 |-- height: string (nullable = true)
 |-- weight: string (nullable = true)

root
 |-- player_api_id: string (nullable = true)
 |-- overall_rating: string (nullable = true)
 |-- finishing: string (nullable = true)
 |-- heading_accuracy: string (nullable = true)
 |-- volleys: string (nullable = true)
 |-- dribbling: string (nullable = true)
 |-- curve: string (nullable = true)
 |-- free_kick_accuracy: string (nullable = true)
 |-- long_passing: string (nullable = true)
 |-- ball_control: string (nullable = true)
 |-- acceleration: string (nullable = true)
 |-- agility: string (nullable = true)
 |-- reactions: string (nullable = true)
 |-- shot_power: string (nullable = true)
 |-- stamina: string (nullable = true)
 |-- strength: string (nullable = true)
 |-- long_shots: string (nullable = true)
 |-- interceptions: string (nullable = true)
 |-- positioning: string (nullable = true)
 |-- vision: string (nullable = true)
 |-- penalties: string (nullable = true)
 |-- marking: string (nullable = true)
 |-- standing_tackle: string (nullable = true)
 |-- sliding_tackle: string (nullable = true)
 |-- gk_diving: string (nullable = true)
 |-- gk_handling: string (nullable = true)
 |-- gk_kicking: string (nullable = true)
 |-- gk_positioning: string (nullable = true)
 |-- gk_reflexes: string (nullable = true)
 |-- year: string (nullable = true)
 
>>> from pyspark.sql.functions import broadcast

>>> players_heading_acc = player_attributes.select("player_api_id","heading_accuracy").join(broadcast(players),player_attributes.player_api_id == players.player_api_id)

>>> players_heading_acc.show(5)

+-------------+----------------+-------------+------------------+--------------+------+------+
|player_api_id|heading_accuracy|player_api_id|       player_name|      birthday|height|weight|
+-------------+----------------+-------------+------------------+--------------+------+------+
|       505942|              71|       505942|Aaron Appindangoye|2/29/1992 0:00|182.88|   187|
|       505942|              71|       505942|Aaron Appindangoye|2/29/1992 0:00|182.88|   187|
|       505942|              71|       505942|Aaron Appindangoye|2/29/1992 0:00|182.88|   187|
|       505942|              70|       505942|Aaron Appindangoye|2/29/1992 0:00|182.88|   187|
|       505942|              70|       505942|Aaron Appindangoye|2/29/1992 0:00|182.88|   187|
+-------------+----------------+-------------+------------------+--------------+------+------+

NOTE:- "player_api_id","heading_accuracy" from player_attributes table and 
		Broadcast players becasue less data and small table , so one copy per node.
		These copies will get stored in nodes cache and join condition as as given in code.
		
>>> players_heading_acc.columns
['player_api_id', 'heading_accuracy', 'player_api_id', 'player_name', 'birthday', 'height', 'weight']

----------------------------------------------
Check whether height affects heading accuracy :-
----------------------------------------------

>>> short_count = sc.accumulator(0)
medium_low_count = sc.accumulator(0)
medium_high_count = sc.accumulator(0)
tall_count = sc.accumulator(0)

NOTE:-  We need to count number of players in each of above given buckets.
		Spark is a distributed Processing system.
		Count variable which will hold the total need to be shared across all compute nodes.
		At this place we need and use ACCUMULATORS.

We will define a function that perform a count operation.
Will take one row as one data point for a perticular player.

def count_players_by_height(row):
	height = float(row.height)
	if (height <= 175):
		short_count.add(1)
	elif (height > 175 and height <= 183):
		medium_low_count.add(1)
	elif (height >183 and height <= 195):
		medium_high_count.add(1)
	elif (height > 195):
		tall_count.add(1)
		
==> To apply this to every player , we will use foreach

>>> players_heading_acc.foreach(lambda i: count_players_by_height(i))

>>> all_players = [short_count.value,medium_low_count.value,medium_high_count.value,tall_count.value]

>>> all_players --------------- is giving wrong result.


short_ha_count = spark.sparkContext.accumulator(0)
medium_low_ha_count = spark.sparkContext.accumulator(0)
medium_high_ha_count = spark.sparkContext.accumulator(0)
tall_ha_count = spark.sparkContext.accumulator(0)


def count_players_by_height_and_heading_accuracy(row, threshhold_score):
	height = row.height
	ha = row.heading_accuracy
	if ha <= threshhold_score:
		return
	if (height <=175):
		short_ha_count.add(1)
	elif (height > 175 and height <=183):
		medium_low_ha_count.add(1)
	elif (height >183 and height  <= 195):
		medium_high_ha_count.add(1)
	elif (height > 195):
		tall_ha_count.add(1)

players_heading_acc.foreach(lambda i: count_players_by_height_and_heading_accuracy(i,60))

all_players_above_threshold = [short_ha_count.value,medium_high_ha_count.value,medium_low_ha_count.value,tall_ha_count.value]

all_players_above_threshold --------------- is giving wrong result.