package com.sg.wrapper

import org.apache.spark.sql.SparkSession

/**
 * A trait to get spark sessions.
 */
trait SparkSessionWrapper {

  /*
 Get spark session for local testing
  */
  lazy val spark: SparkSession = SparkSession
    .builder
    .appName("BigDataKafka")
    .master("local[*]")
    //to fix issue of port assignment on local
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

}
