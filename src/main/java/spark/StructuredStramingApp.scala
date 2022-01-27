package spark

import org.apache.log4j.{Level, Logger}
import breeze.util.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.state
import util.SparkHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StructuredStramingApp extends App {
    private val spark = SparkHelper.getAndConfigureSparkSession()

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


  val clicks = spark
    .readStream
    .format("redis")
    .option("stream.keys", "prod:log:streaming:user"
    ).option("stream.group.name", "my-group")
    .schema(StructType(Array(
      StructField("asset", StringType),
      StructField("cost", StringType)
    ))).load()
  val bypass= clicks.writeStream
      .format("console")
      .start()


  bypass.awaitTermination()


  //  val spark = SparkSession.builder()
//    .master("local")
//    .appName("WordCount1")
//    .getOrCreate()

  import spark.implicits._


//  val lines = spark.readStream
//    .format("socket")
//    .option("host", "localhost")
//    .option("port", 9999)
//    .load()
//  val words = lines.as[String].flatMap(_.split(" "))
//  val wordCounts = words.groupBy("value").count()
//  val query = wordCounts.writeStream
//    .outputMode("complete")
//    .format("console")
//    .start()
//  state.StateStore
//  query.awaitTermination()




  //    val sc=spark.sparkContext
  //    val ssc = new StreamingContext(sc, Seconds(5))
  //    val data=ssc.socketTextStream("localhost",9994)
  //    data.foreachRDD(partRdd=>{
  //      partRdd.foreach(print(_))
  //
  //
  //    })

//  val source = spark
//    .readStream
//    .format("kafka")
//    .option("kafka.bootstrap.servers", "Bigdata01:9092,Bigdata02:9092,Bigdata03:9092")
//    .option("subscribe", "streaming-bank")
//    .option("startingOffsets", "earliest")
//    .load()
//
//    .selectExpr("CAST(value AS STRING)")
//    .as[String]
//
//  val result = source.map {
//    item =>
//      val arr = item.replace("\"", "").split(";")
//      (arr(0).toInt, arr(1).toInt, arr(5).toInt)
//  }
//    .as[(Int, Int, Int)]
//    .toDF("age", "job", "balance")
}
