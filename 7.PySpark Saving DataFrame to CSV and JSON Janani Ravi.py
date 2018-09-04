----------------------------------
Saving DataFrame to CSV and JSON :- Janani Ravi
----------------------------------
----------------
For Spark 2.0 :-
----------------

==> save into a CSV file

>>> pa2016.select("player_api_id","overall_rating").coalesce(1).write.option("header","true").csv("players_overall.csv")

==> Save into JSON file

>>> pa2016.select("player_api_id","overall_rating").write\.json("players_overall.json")


coalesce(1) :- Use to repartition data set into a single partition.
If we omit , the number of file written will be unmber of partition.

----------------
For Spark 1.X :-
----------------

df.write.format("com.databricks.spark.csv").save(path)