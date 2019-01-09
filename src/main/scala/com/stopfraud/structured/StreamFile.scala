package com.stopfraud.structured

import org.apache.ignite.Ignition
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType

import com.stopfraud.common.AnalyseFraud
import com.stopfraud.common.TargetSink
import com.stopfraud.common.UsefulFuncs
import org.apache.spark.sql.streaming.Trigger

object StreamFile {

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
      .json(UsefulFuncs.INPUT_DIR)
      .withColumn(
        "tstamp", to_timestamp(
          from_unixtime(col("unix_time"))));
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

    //val FILE_CONFIG: String = "config/ignite-example-cache.xml"
    //val ignite = Ignition.start(FILE_CONFIG)

    //val writer = TargetSink
    val query: StreamingQuery = wdf.writeStream
      .queryName("stream1")
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime("%s seconds".format(UsefulFuncs.WAITING_IN_SEC)))
      //.foreach(writer)
      .start()

    query.awaitTermination()
    //TargetSink.stop()
  }
}