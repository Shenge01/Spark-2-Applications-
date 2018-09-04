****************************************
JOIN Operation in pyspark by Janani Ravi
****************************************
Janani Ravi
https://app.pluralsight.com/player?course=spark-2-getting-started&author=janani-ravi&name=3a4fd582-8dad-4543-b79e-defc5ca53860&clip=6&mode=live

--------------------------------
Create Data Frame from text file
--------------------------------

>>> playerTxt = sc.textFile("player.txt")

>>> playerTxt_MAP = playerTxt.map(lambda i: (str(i.split(",")[0]),str(i.split(",")[1]),str(i.split(",")[2]),str(i.split(",")[3]),str(i.split(",")[4]),str(i.split(",")[5]),str(i.split(",")[6])))

>>> players = playerTxt_MAP.toDF(schema=["id","player_api_id","player_name","player_fifa_api_id","birthday","height","weight"])

>>> players.printSchema()
root
 |-- id: string (nullable = true)
 |-- player_api_id: string (nullable = true)
 |-- player_name: string (nullable = true)
 |-- player_fifa_api_id: string (nullable = true)
 |-- birthday: string (nullable = true)
 |-- height: string (nullable = true)
 |-- weight: string (nullable = true)

>>> player_attributesTxt = sc.textFile("player_attributes.txt")

>>> player_attributes_MAP = player_attributesTxt.map(lambda i: (str(i.split(",")[0]),str(i.split(",")[1]),\
str(i.split(",")[2]),str(i.split(",")[3]),str(i.split(",")[4]),str(i.split(",")[5]),str(i.split(",")[6]),\
str(i.split(",")[7]),str(i.split(",")[8]),str(i.split(",")[9]),str(i.split(",")[10]),str(i.split(",")[11]),\
str(i.split(",")[12]),str(i.split(",")[13]),str(i.split(",")[14]),str(i.split(",")[15]),str(i.split(",")[16]),\
str(i.split(",")[17]),str(i.split(",")[18]),str(i.split(",")[19]),str(i.split(",")[20]),str(i.split(",")[21]),\
str(i.split(",")[22]),str(i.split(",")[23]),str(i.split(",")[24]),str(i.split(",")[25]),str(i.split(",")[26]),\
str(i.split(",")[27]),str(i.split(",")[28]),str(i.split(",")[29]),str(i.split(",")[30]),str(i.split(",")[31]),\
str(i.split(",")[32]),str(i.split(",")[33]),str(i.split(",")[34]),str(i.split(",")[35]),str(i.split(",")[36]),\
str(i.split(",")[37]),str(i.split(",")[38]),str(i.split(",")[39]),str(i.split(",")[40]),str(i.split(",")[41])))

>>> player_attributes = player_attributes_MAP.toDF(schema=["id",
	"player_fifa_api_id",
	"player_api_id",
	"date",
	"overall_rating",
	"potential",
	"preferred_foot",
	"attacking_work_rat",
	"defensive_work_rat",
	"crossing",
	"finishing",
	"heading_accuracy",
	"short_passing",
	"volleys",
	"dribbling",
	"curve",
	"free_kick_accuracy",
	"long_passing",
	"ball_control",
	"acceleration",
	"sprint_speed",
	"agility",
	"reactions",
	"balance",
	"shot_power",
	"jumping",
	"stamina",
	"strength",
	"long_shots",
	"aggression",
	"interceptions",
	"positioning",
	"vision",
	"penalties",
	"marking",
	"standing_tackle",
	"sliding_tackle",
	"gk_diving",
	"gk_handling",
	"gk_kicking",
	"gk_positioning",
	"gk_reflexes"])

NOTE:-- We can use BroadCast variable to limit number of copies of our data on worker node.
		When you have one data frame much smaller than other.
		players has 11k records and player_attributes has 18k records.

>>> player_attributes.printSchema()
root
 |-- id: string (nullable = true)
 |-- player_fifa_api_id: string (nullable = true)
 |-- player_api_id: string (nullable = true)
 |-- date: string (nullable = true)
 |-- overall_rating: string (nullable = true)
 |-- potential: string (nullable = true)
 |-- preferred_foot: string (nullable = true)
 |-- attacking_work_rat: string (nullable = true)
 |-- defensive_work_rat: string (nullable = true)
 |-- crossing: string (nullable = true)
 |-- finishing: string (nullable = true)
 |-- heading_accuracy: string (nullable = true)
 |-- short_passing: string (nullable = true)
 |-- volleys: string (nullable = true)
 |-- dribbling: string (nullable = true)
 |-- curve: string (nullable = true)
 |-- free_kick_accuracy: string (nullable = true)
 |-- long_passing: string (nullable = true)
 |-- ball_control: string (nullable = true)
 |-- acceleration: string (nullable = true)
 |-- sprint_speed: string (nullable = true)
 |-- agility: string (nullable = true)
 |-- reactions: string (nullable = true)
 |-- balance: string (nullable = true)
 |-- shot_power: string (nullable = true)
 |-- jumping: string (nullable = true)
 |-- stamina: string (nullable = true)
 |-- strength: string (nullable = true)
 |-- long_shots: string (nullable = true)
 |-- aggression: string (nullable = true)
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
 
 
 
>>> players.count() , player_attributes.count()

(11060, 183978)

>>> player_attributes.select('player_api_id').distinct().count()

11060

>>> players = players.drop("id")

>>> players = players.drop("player_fifa_api_id")

>>> players.columns
['player_api_id', 'player_name', 'birthday', 'height', 'weight']

>>> player_attributes = player_attributes.drop("id")
player_attributes = player_attributes.drop("player_fifa_api_id")
player_attributes = player_attributes.drop("preferred_foot")
player_attributes = player_attributes.drop("attacking_work_rat")
player_attributes = player_attributes.drop("defensive_work_rat")
player_attributes = player_attributes.drop("crossing")
player_attributes = player_attributes.drop("jumping")
player_attributes = player_attributes.drop("sprint_speed")
player_attributes = player_attributes.drop("balance")
player_attributes = player_attributes.drop("aggression")
player_attributes = player_attributes.drop("short_passing")
player_attributes = player_attributes.drop("potential")

NOTE :- .drop("","","") is not working , giving error [TypeError: drop() takes exactly 2 arguments (3 given)].
		so droping one by one as above. 
		
>>> player_attributes = player_attributes.dropna()
players = players.dropna()

To set up User Defined Function:-

>>> from pyspark.sql.functions import udf

>>> year_extract_udf = udf(lambda i: i.split(' ')[0].split('/')[2])

>>> player_attributes = player_attributes.withColumn("year",year_extract_udf(player_attributes.date))

>>> player_attributes.show(1)
+-------------+--------------+--------------+---------+----------------+-------------+-------+---------+-----+------------------+------------+------------+------------+-------+---------+-------+--------+----------+-------------+-----------+------+---------+-------+---------------+--------------+---------+-----------+----------+--------------+-----------+----+
|player_api_id|          date|overall_rating|finishing|heading_accuracy|short_passing|volleys|dribbling|curve|free_kick_accuracy|long_passing|ball_control|acceleration|agility|reactions|stamina|strength|long_shots|interceptions|positioning|vision|penalties|marking|standing_tackle|sliding_tackle|gk_diving|gk_handling|gk_kicking|gk_positioning|gk_reflexes|year|
+-------------+--------------+--------------+---------+----------------+-------------+-------+---------+-----+------------------+------------+------------+------------+-------+---------+-------+--------+----------+-------------+-----------+------+---------+-------+---------------+--------------+---------+-----------+----------+--------------+-----------+----+
|       505942|2/18/2016 0:00|            67|       44|              71|           61|     44|       51|   45|                39|          64|          49|          60|     59|       47|     54|      76|        35|           70|         45|    54|       48|     65|             69|            69|        6|         11|        10|             8|          8|2016|
+-------------+--------------+--------------+---------+----------------+-------------+-------+---------+-----+------------------+------------+------------+------------+-------+---------+-------+--------+----------+-------------+-----------+------+---------+-------+---------------+--------------+---------+-----------+----------+--------------+-----------+----+
only showing top 1 row

>>> player_attributes = player_attributes.drop("date")

>>> pa2016 = player_attributes.filter(player_attributes.year == 2016)

>>> pa2016.count()
14103

>>> pa2016.select(pa2016.player_api_id).distinct().count()

5586

>>> pa2016.printSchema()
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
 

>>> pa_striker_2016 = pa2016.groupBy('player_api_id').agg({'finishing':'avg','shot_power':'avg','acceleration':'avg'})

>>> pa_striker_2016.show(3)

+-------------+---------------+-----------------+--------------+
|player_api_id|avg(shot_power)|avg(acceleration)|avg(finishing)|
+-------------+---------------+-----------------+--------------+
|        31916|           75.0|             62.0|          75.0|
|       131409|           84.0|             69.0|          63.0|
|       177689|           42.0|             69.0|          33.0|
+-------------+---------------+-----------------+--------------+
only showing top 3 rows

NOTE:- Using agg method we can do aggrigation with multiple columns.

>>> pa_striker_2016 = pa_striker_2016.withColumnRenamed('avg(finishing)','FINISHING').withColumnRenamed('avg(shot_power)','SHOT_POWER').withColumnRenamed('avg(acceleration)','ACCELERATION')

+-------------+----------+------------+---------+
|player_api_id|SHOT_POWER|ACCELERATION|FINISHING|
+-------------+----------+------------+---------+
|        31916|      75.0|        62.0|     75.0|
|       131409|      84.0|        69.0|     63.0|
+-------------+----------+------------+---------+

>>> weight_finishing = 1
weight_shot_power = 2
weight_acceleration = 1

total_weight = weight_finishing + weight_shot_power + weight_acceleration

>>> strikers = pa_striker_2016.withColumn('striker_grade',(pa_striker_2016.FINISHING * weight_finishing +pa_striker_2016.SHOT_POWER * weight_shot_power +pa_striker_2016.ACCELERATION * weight_acceleration) / total_weight)

>>> strikers = strikers.drop('SHOT_POWER')
strikers = strikers.drop('ACCELERATION')
strikers = strikers.drop('FINISHING')

>>> strikers = strikers.filter(strikers.striker_grade > 70).sort(strikers.striker_grade.desc())
>>> strikers.show()

+-------------+-------------+
|player_api_id|striker_grade|
+-------------+-------------+
|        20276|        89.25|
|        37412|         89.0|
|        38817|        88.75|
+-------------+-------------+

NOTE:- To know the names of players we need to JOIN this table(data set) with players data frame.

>>> strikers.count(), players.count()

(1609, 11060)

>>> players.columns
['player_api_id', 'player_name', 'birthday', 'height', 'weight']

>>> strikers.columns
['player_api_id', 'striker_grade']

>>> striker_details = players.join(strikers, players.player_api_id == strikers.player_api_id)

>>> striker_details.printSchema()
root
 |-- player_api_id: string (nullable = true)
 |-- player_name: string (nullable = true)
 |-- birthday: string (nullable = true)
 |-- height: string (nullable = true)
 |-- weight: string (nullable = true)
 |-- player_api_id: string (nullable = true)
 |-- striker_grade: double (nullable = true)

>>> striker_details.show()

+-------------+--------------------+---------------+------+------+-------------+-----------------+
|player_api_id|         player_name|       birthday|height|weight|player_api_id|    striker_grade|
+-------------+--------------------+---------------+------+------+-------------+-----------------+
|       114651|              Mateus| 6/19/1984 0:00|175.26|   165|       114651|            76.75|
|        12386|    Krisztian Nemeth|  1/5/1989 0:00|180.34|   163|        12386|             72.5|
|       131409|    Adlene Guedioura|11/12/1985 0:00| 177.8|   179|       131409|             75.0|
|       181242|      Marvin Sordell| 2/17/1991 0:00| 177.8|   179|       181242|             70.5|
|       209430|Konstantinos Fort...|10/16/1992 0:00|182.88|   154|       209430|            74.25|

NOTE:- Using this JOIN we can see <player_api_id> are in reslut data set coming two times.

>>> striker_details.count()

1609

CHANGING JOIN OPERATION SYNTEX:-

Using Inner join:-


>>> striker_details = players.join(strikers, ['player_api_id'])

+-------------+--------------------+---------------+------+------+-----------------+
|player_api_id|         player_name|       birthday|height|weight|    striker_grade|
+-------------+--------------------+---------------+------+------+-----------------+
|       114651|              Mateus| 6/19/1984 0:00|175.26|   165|            76.75|
|        12386|    Krisztian Nemeth|  1/5/1989 0:00|180.34|   163|             72.5|
|       131409|    Adlene Guedioura|11/12/1985 0:00| 177.8|   179|             75.0|

NOTE:- Changing syntex of join operation , we can see we are getting <player_api_id> onle once.





