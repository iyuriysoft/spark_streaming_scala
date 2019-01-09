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

// 300 s stream output looks like that:
//+-------------------+-------------------+----------+-------+---+------------------+----------+-----------+
//|2019-01-06 12:53:00|2019-01-06 12:51:00|172.20.0.1|20     |60 |4.0               |1546768380|1546768260 |
//|2019-01-06 12:53:00|2019-01-06 12:51:00|172.20.0.0|19     |60 |3.0               |1546768380|1546768260 |
//|2019-01-06 12:52:00|2019-01-06 12:50:00|172.20.0.1|19     |60 |4.0               |1546768320|1546768200 |
//|2019-01-06 12:52:00|2019-01-06 12:50:00|172.20.0.0|19     |60 |4.454545454545454 |1546768320|1546768200 |
//|2019-01-06 12:51:00|2019-01-06 12:49:00|172.20.0.1|19     |60 |3.6153846153846154|1546768260|1546768140 |
//|2019-01-06 12:51:00|2019-01-06 12:49:00|172.20.0.0|20     |60 |4.454545454545454 |1546768260|1546768140 |
//+-------------------+-------------------+----------+-------+---+------------------+----------+-----------+

object StreamKafka {

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
      UsefulFuncs.THRESHOLD_COUNT_IP,
      UsefulFuncs.THRESHOLD_COUNT_UNIQ_CATEGORY,
      UsefulFuncs.THRESHOLD_CLICK_VIEW_RATIO,
      UsefulFuncs.WIN_WATERMARK_IN_SEC,
      UsefulFuncs.WIN_DURATION_IN_SEC,
      UsefulFuncs.WIN_SLIDE_DURATION_IN_SEC);
    wdf.printSchema();

    val query: StreamingQuery = wdf.writeStream
      .queryName("stream1")
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime("%s seconds".format(UsefulFuncs.WAITING_IN_SEC)))
      //.foreach(writer.instance())
      .start()

    query.awaitTermination()
    //TargetSink.stop()
  }
}