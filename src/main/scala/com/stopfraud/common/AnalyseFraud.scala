package com.stopfraud.common

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object AnalyseFraud {

  def getFilterData(
    df: Dataset[Row],
    threshold: Int, thresholdCategories: Int, thresholdRatio: Double,
    delayThreshold: Int, windowDuration: Int, slideDuration: Int): Dataset[Row] = {

    // Apply event-time window
    val wdf = df
      .withWatermark("tstamp", "%d seconds".format(delayThreshold))
      .groupBy(
        functions.window(
          col("tstamp"),
          "%d seconds".format(windowDuration),
          "%d seconds".format(slideDuration)),
        col("ip"))
      .agg(
        count("*").alias("cnt"),
        collect_list("type").alias("types"),
        collect_list("category_id").alias("categories"))
      .sort(desc("cnt"), desc("window.end"))
    //.orderBy(col("cnt").desc(), col("window.end").desc())

    // Apply UDF
    val wdf2 = wdf.select(
      col("*"),
      callUDF("getDevided", col("types")),
      callUDF("getUniqCount", col("categories")))
      .withColumnRenamed("UDF:getUniqCount(categories)", "uniqCnt")
      .withColumnRenamed("UDF:getDevided(types)", "ratio");

    // Apply restrictive filter
    val wdf3 = wdf2.select(
      col("window.end"), col("window.start"), col("ip"), col("uniqCnt"), col("cnt"), col("ratio"))
      .where(col("cnt").$greater(threshold))
      .where(col("uniqCnt").$greater(thresholdCategories))
      .where(col("ratio").$greater(thresholdRatio))
      .withColumn("utime_end", unix_timestamp(col("end")))
      .withColumn("utime_start", unix_timestamp(col("start")));
    wdf3
  }

  def getInputSchema(): StructType = {
    StructType(Array(
      StructField("ip", StringType),
      StructField("unix_time", LongType),
      StructField("type", StringType),
      StructField("category_id", IntegerType)))
  }

  //  def getInputSchema(): StructType = {
  //    new StructType()
  //      .add("ip", "string")
  //      .add("unix_time", "long")
  //      .add("type", "string")
  //      .add("category_id", "int");
  //  }

}