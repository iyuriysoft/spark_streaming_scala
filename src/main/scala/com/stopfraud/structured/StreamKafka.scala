package com.stopfraud.structured

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType

import com.stopfraud.common.AnalyseFraud
import com.stopfraud.common.UsefulFuncs

object StreamKafka {
  private final val WAITING_IN_SEC = 60;
  private final val WIN_WATERMARK_IN_SEC = 300;
  private final val WIN_DURATION_IN_SEC = 120;
  private final val WIN_SLIDE_DURATION_IN_SEC = 60;
  private final val THRESHOLD_COUNT_IP = 59;
  private final val THRESHOLD_COUNT_UNIQ_CATEGORY = 15;
  private final val THRESHOLD_CLICK_VIEW_RATIO = 2.5;

  def main(args: Array[String]): Unit = {
    println("Streaming Kafka")
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    val spark = SparkSession
      .builder
      .appName("SparkStreamingKafka")
      .master("local[2]")
      .config("spark.eventLog.enabled", "false")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .getOrCreate();

    UsefulFuncs.setupUDFs(spark)

    val brokers = "localhost:9092"
    val topics = "firsttopic"
    startJobKafka(spark, AnalyseFraud.getInputSchema(), brokers, topics)
    spark.stop()
  }
  
  private def startJobKafka(spark: SparkSession, schema: StructType,
    brokers: String, topics: String): Unit = {

    // Create a Dataset representing the stream of input files
    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .load()
      .selectExpr("cast(value as string)")
      .withColumn("tokens", from_json(col("value"), schema))
      .withColumn("unix_time", col("tokens").getItem("unix_time").cast("long"))
      .withColumn(
        "tstamp",
        to_timestamp(
          from_unixtime(col("unix_time")))) // Event time has to be a timestamp
      .withColumn("category_id", col("tokens").getItem("category_id").cast("int"))
      .withColumn("ip", col("tokens").getItem("ip"))
      .withColumn("type", col("tokens").getItem("type"));
    stream.printSchema();

    val wdf = AnalyseFraud.getFilterData(
      stream,
      THRESHOLD_COUNT_IP,
      THRESHOLD_COUNT_UNIQ_CATEGORY,
      THRESHOLD_CLICK_VIEW_RATIO,
      WIN_WATERMARK_IN_SEC,
      WIN_DURATION_IN_SEC,
      WIN_SLIDE_DURATION_IN_SEC);
    wdf.printSchema();

    val query: StreamingQuery = wdf.writeStream
      .queryName("stream1")
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime("%s seconds".format(WAITING_IN_SEC)))
      //.foreach(writer.instance())
      .start()

    query.awaitTermination()
    //TargetSink.stop()
  }
}