import scala.collection.Seq._
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.functions.udf
import spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, FloatType}
val sqlContext=new SQLContext(sc)
val names_lists=movie_names_df.select("name").rdd.map(r=>r(0))

val names_rdd=names_lists.map{case a:Traversable[_]=>a.map{_.toString}.toSeq.sorted}.filter(_.size>1)

val names_pairs=names_rdd.flatMap(list=>list.combinations(2))

val names_pairs_count=names_pairs.map(x=>(x,1)).reduceByKey(_+_)

val names_pairs_df=names_pairs_count.map{case (a:Seq[String],b:Int)=>(a(0),a(1),b)}.toDF("name1","name2","count").orderBy("name1","name2").alias("actors")

val name_count_df=name_movie_df.withColumn("count",size(name_movie_df("movie"))).drop("movie").orderBy(asc("name"))

names_pairs_df.coalesce(1).write.format("com.databricks.spark.csv").save("names_pairs.csv")
name_count_df=coalesce(1).write.format("com.databricks.spark.csv").save("name_count.csv")




