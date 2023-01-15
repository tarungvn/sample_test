package org.example.util

import org.apache.spark.sql.SparkSession

object SessionSetup {

  def sparkSession(master: String, appName: String): SparkSession = {

    val spark: SparkSession = SparkSession.builder()
      .master(master)
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KyroSerializer")
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.sql.broadcastTimeout", "1200")
      .enableHiveSupport()
      .getOrCreate()

     spark
  }
}
