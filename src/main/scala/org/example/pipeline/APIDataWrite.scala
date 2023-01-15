package org.example.pipeline

import org.apache.spark.sql.DataFrame

object APIDataWrite {

  def writeData(df:DataFrame,writePath:String): Unit=
    {
      df.repartition(2).write.mode("overwrite").parquet(writePath)
    }

}
