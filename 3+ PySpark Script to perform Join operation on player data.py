***********************************************
Script to perform Join operation on player data:- 
***********************************************

git clone https://github.com/rudrasingh21/Data-For-Spark.git

playerTxt = sc.textFile("/player.txt")
#playerTxt = sc.textFile("/home/ubuntu/Data-For-Spark/player.txt")
playerTxt_MAP = playerTxt.map(lambda i: (str(i.split(",")[0]),str(i.split(",")[1]),str(i.split(",")[2]),str(i.split(",")[3]),str(i.split(",")[4]),str(i.split(",")[5]),str(i.split(",")[6])))
players = playerTxt_MAP.toDF(schema=["id","player_api_id","player_name","player_fifa_api_id","birthday","height","weight"])
player_attributesTxt = sc.textFile("/player_attributes.txt")
#player_attributesTxt = sc.textFile("/home/ubuntu/Data-For-Spark/player_attributes.txt")
player_attributes_MAP = player_attributesTxt.map(lambda i: (str(i.split(",")[0]),str(i.split(",")[1]),\
str(i.split(",")[2]),str(i.split(",")[3]),str(i.split(",")[4]),str(i.split(",")[5]),str(i.split(",")[6]),\
str(i.split(",")[7]),str(i.split(",")[8]),str(i.split(",")[9]),str(i.split(",")[10]),str(i.split(",")[11]),\
str(i.split(",")[12]),str(i.split(",")[13]),str(i.split(",")[14]),str(i.split(",")[15]),str(i.split(",")[16]),\
str(i.split(",")[17]),str(i.split(",")[18]),str(i.split(",")[19]),str(i.split(",")[20]),str(i.split(",")[21]),\
str(i.split(",")[22]),str(i.split(",")[23]),str(i.split(",")[24]),str(i.split(",")[25]),str(i.split(",")[26]),\
str(i.split(",")[27]),str(i.split(",")[28]),str(i.split(",")[29]),str(i.split(",")[30]),str(i.split(",")[31]),\
str(i.split(",")[32]),str(i.split(",")[33]),str(i.split(",")[34]),str(i.split(",")[35]),str(i.split(",")[36]),\
str(i.split(",")[37]),str(i.split(",")[38]),str(i.split(",")[39]),str(i.split(",")[40]),str(i.split(",")[41])))
player_attributes = player_attributes_MAP.toDF(schema=["id",
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
players = players.drop("id")
players = players.drop("player_fifa_api_id")

#NOTE:- players = players.drop("id","player_fifa_api_id") -- Working in spark 2

player_attributes = player_attributes.drop("id")
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
player_attributes = player_attributes.dropna()
players = players.dropna()
from pyspark.sql.functions import udf
year_extract_udf = udf(lambda i: i.split(' ')[0].split('/')[2])
player_attributes = player_attributes.withColumn("year",year_extract_udf(player_attributes.date))
player_attributes = player_attributes.drop("date")
pa2016 = player_attributes.filter(player_attributes.year == 2016)
pa_striker_2016 = pa2016.groupBy('player_api_id').agg({'finishing':'avg','shot_power':'avg','acceleration':'avg'})
pa_striker_2016 = pa_striker_2016.withColumnRenamed('avg(finishing)','FINISHING').withColumnRenamed('avg(shot_power)','SHOT_POWER').withColumnRenamed('avg(acceleration)','ACCELERATION')
weight_finishing = 1
weight_shot_power = 2
weight_acceleration = 1
total_weight = weight_finishing + weight_shot_power + weight_acceleration
strikers = pa_striker_2016.withColumn('striker_grade',(pa_striker_2016.FINISHING * weight_finishing + pa_striker_2016.SHOT_POWER * weight_shot_power +pa_striker_2016.ACCELERATION * weight_acceleration) / total_weight)
strikers = strikers.drop('SHOT_POWER')
strikers = strikers.drop('ACCELERATION')
strikers = strikers.drop('FINISHING')
strikers = strikers.filter(strikers.striker_grade > 70).sort(strikers.striker_grade.desc())
striker_details = players.join(strikers, players.player_api_id == strikers.player_api_id)
