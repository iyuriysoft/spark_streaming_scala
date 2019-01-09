package com.stopfraud.common

import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row

//import java.lang.{Long ⇒ JLong, String ⇒ JString}
/*
case class TargetSinkA(var ignite: Ignite, var ccfg: CacheConfiguration[String, Long]) extends ForeachWriter[Row] {

  //  val CACHE_NAME: String = "myCache"
  //  val FILE_CONFIG: String = "config/ignite-example-cache.xml"
  //var ignite: Ignite = null
  //var ccfg: CacheConfiguration[String, Long] = null
  //  val ignite = Ignition.start(FILE_CONFIG)
  //  val ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
  //    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)

  //  def init() = {
  //    ignite = Ignition.start(FILE_CONFIG)
  //    ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
  //    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
  //
  //    state += 1
  //    System.out.println("I did something for the " + this.state + " time")
  //  }

  def open(partitionId: Long, version: Long): Boolean = {
    println("============================================================")
    println("PARTITION:%d, VERSION:%d, Thread:%d".format(partitionId, version,
      Thread.currentThread().getId()))
    println(if (ignite == null) "ignite null" else ignite.toString())
    //println(if (ccfg == null) "ccfg null " else ccfg.toString())
    //    if (ignite == null)
    //      ignite = Ignition.start(FILE_CONFIG)
    //    if (ccfg == null)
    //      ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
    //        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
    true
  }

  def process(value: Row): Unit = {
    val CACHE_NAME: String = "myCache"
    val ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
    println("PROCESS:%s, Thread:%d".format(value, Thread.currentThread().getId()))
    ignite.getOrCreateCache(ccfg).put(value.getAs("ip"), value.getAs("utime_end"));
  }

  def close(errorOrNull: Throwable): Unit = {
    println("CLOSE, Thread:%d".format(Thread.currentThread().getId()));
  }
}

object TargetSink {
  val CACHE_NAME: String = "myCache"
  val FILE_CONFIG: String = "config/ignite-example-cache.xml"
  //  val ignite = Ignition.start(FILE_CONFIG)
  //  val ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
  //    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
  var ignite: Ignite = null
  //var ccfg: CacheConfiguration[String, Long] = null

  private var _instance: TargetSinkA = null

  def instance() = {
    if (_instance == null) {
      ignite = Ignition.start(FILE_CONFIG)
      //ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
      //  .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
      _instance = new TargetSinkA(ignite, null)
    }
    _instance
  }

  def stop(): Unit = {
    println("stop Thread:%d".format(Thread.currentThread().getId()));
    if (ignite != null)
      ignite.close();
    Ignition.stop(true);
  }
}
*/
/*
case class TargetSink(var ignite: Ignite) extends ForeachWriter[Row] {

  val CACHE_NAME: String = "myCache"
  val FILE_CONFIG: String = "config/ignite-example-cache.xml"
  //var ignite: Ignite = null
  //var ccfg: CacheConfiguration[String, Long] = null
  //val ignite = Ignition.start(FILE_CONFIG)
  //val ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
  //  .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
  
  def open(partitionId: Long, version: Long): Boolean = {
    println("============================================================")
    println(("PARTITION:%d, VERSION:%d, Thread:%d".format(partitionId, version,
      Thread.currentThread().getId())))
    println(if (ignite == null) "ignite null" else ignite.toString())
    //println(if (ccfg == null) "ccfg null " else ccfg.toString())
//    if (ignite == null)
//      ignite = Ignition.start(FILE_CONFIG)
//    if (ccfg == null)
//      ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
//        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
    true
  }

  def process(value: Row): Unit = {
    val ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
    println("PROCESS:%s, Thread:%d".format(value, Thread.currentThread().getId()))
    val tm: Long = value.getAs("utime_end")
    ignite.getOrCreateCache(ccfg).put(value.getAs("ip"), tm);
  }

  def close(errorOrNull: Throwable): Unit = {
    //stop()
    println("CLOSE, Thread:%d".format(Thread.currentThread().getId()));
  }

  def stop(): Unit = {
    println("stop Thread:%d".format(Thread.currentThread().getId()));
    if (ignite != null)
      ignite.close();
    Ignition.stop(true);
  }
}
*/

object TargetSink extends ForeachWriter[Row] {

  val CACHE_NAME: String = "myCache"
  val FILE_CONFIG: String = "config/ignite-example-cache.xml"
  val ignite = Ignition.start(FILE_CONFIG)
  val ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
  
  def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  def process(value: Row): Unit = {
    val ccfg = new CacheConfiguration[String, Long](CACHE_NAME).setSqlSchema("PUBLIC")
        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
    println("PROCESS:%s, Thread:%d".format(value, Thread.currentThread().getId()))
    ignite.getOrCreateCache(ccfg).put(value.getAs("ip"), value.getAs("utime_end"));
  }

  def close(errorOrNull: Throwable): Unit = {
    println("CLOSE, Thread:%d".format(Thread.currentThread().getId()));
  }

  def stop(): Unit = {
    println("stop Thread:%d".format(Thread.currentThread().getId()));
    if (ignite != null)
      ignite.close();
    Ignition.stop(true);
  }
}
