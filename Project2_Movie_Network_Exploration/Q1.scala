import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.functions.{collect_list, collect_set}


val sqlContext: SQLContext = new HiveContext(sc)

val actor_context=sc.textFile("actor_movies.txt")
val actor_movies=actor_context.map(_.split("\t\t")).filter(_.length>10).map(x=>(x(0),x.drop(1)))
val actress_context=sc.textFile("actress_movies.txt")
val actress_movies=actress_context.map(_.split("\t\t")).filter(_.length>10).map(x=>(x(0),x.drop(1)))
val name_movie=actor_movies.union(actress_movies)
val name_movie_df=name_movie.toDF("name","movie")

val movie_name=name_movie.flatMap{case(name,movies)=>movies.map(_->name)}
val movie_name_df=movie_name.toDF("movie","name")
val movie_names_df=movie_name_df.groupBy($"movie").agg(collect_list($"name").alias("name"))
movie_names_df.cache()
name_movie_df.cache()
val movieID_names_df=movie_names_df.withColumn("id",monotonically_increasing_id())
val movie_dict=movieID_names_df.select("movie","id")


