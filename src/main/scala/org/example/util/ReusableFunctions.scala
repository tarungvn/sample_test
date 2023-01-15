package org.example.util
import java.io.{BufferedReader, InputStreamReader, Reader}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

object ReusableFunctions {

def getHdfsReader(filePath:String)(sc:SparkContext): Reader ={
  val fs=FileSystem.get(sc.hadoopConfiguration)
  val path=new Path(filePath)
  new BufferedReader(new InputStreamReader(fs.open(path)))
}

}
