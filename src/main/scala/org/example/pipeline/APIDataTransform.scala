package org.example.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, explode}


object APIDataTransform {

  def transformAPIData(sparkSession:SparkSession,readDf:DataFrame): DataFrame =
  {
    val explodeDf=readDf.select(explode(col("bikes")))
    val transformDf=explodeDf.select(col("col.*"))
    val targetDf=transformDf.withColumn("insert_date",current_timestamp())
    targetDf
  }
}
