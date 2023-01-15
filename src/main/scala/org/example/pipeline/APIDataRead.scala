package org.example.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}


object APIDataRead {

def readData(sparkSession: SparkSession,incident_url:String,endpoint:String):DataFrame={
  val jsonString=scala.io.Source.fromURL(incident_url+endpoint).mkString.replace("\n//","")
  import sparkSession.implicits._
  val df=sparkSession.read.option("multiline","true").option("mode","PERMISSIVE").json(Seq(jsonString).toDS)
  df
}

}
