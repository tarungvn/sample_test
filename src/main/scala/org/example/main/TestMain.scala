package org.example.main
import org.example.util.SessionSetup
import com.typesafe.config.ConfigFactory
import org.example.util.ReusableFunctions
import org.example.pipeline.APIDataRead
import org.example.pipeline.APIDataTransform
import org.example.pipeline.APIDataWrite
import java.io.Reader
import org.slf4j.LoggerFactory
object TestMain {

def main(args:Array[String]) : Unit=
  {

    /*args frm spark submit
    * */

    val configFilePath= args(0)
    val master=args(1)
    val appName=args(2)



    /*Create a Spark Session*/

    val sparkSession= SessionSetup.sparkSession(master, appName)

   /*Read the Configuration File*/

    val reader:Reader= ReusableFunctions.getHdfsReader(configFilePath)(sparkSession.sparkContext)
    val config=ConfigFactory.parseReader(reader)

    /*Read the variables from configuration file*/

    val incident_url=config.getString("test.incident_url")
    val end_point=config.getString("test.endpoint")
    val writepath=config.getString("test.writepath")

    val log=LoggerFactory.getLogger(this.getClass.getName)

    try {

      /* Read Data from API*/

      log.info("Read data from url "+incident_url+end_point)

      val readDf = APIDataRead.readData(sparkSession, incident_url, end_point)

      /*Transform Data from API*/
      log.info("Transform API Data")
      val transformDf = APIDataTransform.transformAPIData(sparkSession, readDf)

      /*Write Data*/
      log.info("Write API Data")
      APIDataWrite.writeData(transformDf, writepath)

    }
    catch
      {
        case ex :Throwable  =>println("Exception Occurred")
      }

  }
}
