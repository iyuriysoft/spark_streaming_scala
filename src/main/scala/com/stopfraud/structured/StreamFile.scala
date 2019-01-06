package com.stopfraud.structured

import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType

import com.stopfraud.common.AnalyseFraud
import com.stopfraud.common.UsefulFuncs
import com.stopfraud.common.TargetSinkA
import com.stopfraud.common.TargetSink

object StreamFile {
  private final val INPUT_DIR = "/Users/Shared/test/fraud";
  private final val WAITING_IN_SEC = 60;
  private final val WIN_WATERMARK_IN_SEC = 300;
  private final val WIN_DURATION_IN_SEC = 120;
  private final val WIN_SLIDE_DURATION_IN_SEC = 60;
  private final val THRESHOLD_COUNT_IP = 59;
  private final val THRESHOLD_COUNT_UNIQ_CATEGORY = 15;
  private final val THRESHOLD_CLICK_VIEW_RATIO = 2.5;

  def main(args: Array[String]): Unit = {
    println("Streaming File")
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    val spark = SparkSession
      .builder
      .appName("SparkStreamingFile")
      .master("local[2]")
      .config("spark.eventLog.enabled", "false")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    UsefulFuncs.setupUDFs(spark)
    startJobFile(spark, AnalyseFraud.getInputSchema())
    spark.stop()
  }

  private def startJobFile(spark: SparkSession, schema: StructType) {

    // Create a Dataset representing the stream of input files
    val stream = spark
      .readStream
      .schema(schema)
      .json(INPUT_DIR)
      .withColumn(
        "tstamp", to_timestamp(
          from_unixtime(col("unix_time"))));
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