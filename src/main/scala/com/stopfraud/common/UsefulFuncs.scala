package com.stopfraud.common

import scala.collection.mutable.Set

import org.apache.spark.sql.SparkSession

object UsefulFuncs {
  private var value1: String = "click"
  private var value2: String = "view"

  private def initRatio(val1: String, val2: String) {
    value1 = val1
    value2 = val2
  }

  private def calcRatio(list: Seq[String]): Double = {
    if (value1.isEmpty() || value2.isEmpty()) {
      throw new RuntimeException("It needs to call init() before!")
    }
    if (list == null) {
      -1.0
    }
    var cnt1:Double = 0; var cnt2 = 0
    for (s <- list) {
      if (s.equalsIgnoreCase(value1)) {
        cnt1 = cnt1 + 1
      } else if (s.equalsIgnoreCase(value2)) {
        cnt2 = cnt2 + 1
      }
    }
    cnt1 / cnt2
  }

  private def calcUniqCount(ar: Seq[Int]): Int = {
    if (ar == null) {
      -1
    }
    var se: Set[Int] = Set()
    for (it <- ar) { se.add(it) }
    se.size
  }

  def setupUDFs(spark: SparkSession) = {
    initRatio("click", "view");
    spark.udf.register("getDevided", calcRatio(_: Seq[String]))
    spark.udf.register("getUniqCount", calcUniqCount(_: Seq[Int]));
  }
}
