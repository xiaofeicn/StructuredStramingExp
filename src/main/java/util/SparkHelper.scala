package util

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkHelper {
  def getAndConfigureSparkSession() = {

    val props = PropertiesManager.getUtil

    val conf = new SparkConf()
      .setAppName("Structured Streaming")
      .set("spark.redis.host","172.30.134.36")
      .set("spark.redis.port","6380")
      .set("spark.redis.db", "21")
      .set("spark.redis.auth","huanshuo@2020")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    SparkSession
      .builder()
      .getOrCreate()
  }

  def getSparkSession() = {
    SparkSession
      .builder()
      .getOrCreate()
  }
}
