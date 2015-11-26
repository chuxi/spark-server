package cn.edu.zju.king.jobserver.etl

import java.nio.file.{Files, Paths}

import cn.edu.zju.king.serverapi.{SparkJobValid, SparkJobInvalid, SparkJob, SparkJobValidation}
import com.typesafe.config.Config
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.util.Try

/**
  * Created by king on 15-11-19.
  */
object ExtractionApp extends SparkJob {
  private val logger = LogManager.getLogger(getClass)
  // should be hdfs:// or file:// protocal file
  private val datasourceString = "datasource"
  // define the etl config
  private val etlConfigString = "etlconfig"
  // should be JsArray, define the fields to extract from the source
  private val extractConfigString = "extract"
  // should be JsArray, every element defines how to transform a field into another
  private val transformConfigString = "transform"
  // maybe a sql sentence
  private val loadConfigString = "load"

  def main(args: Array[String]) {

  }

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
//    val df = sqlc.read.json("algolibs/src/main/resources/student.json")
    val df = sqlc.read.json(getClass.getResource("/student.json").getPath)
    df.printSchema()
    df.show()
    df.groupBy("gender").count().show()
  }

  /**
    * validate the config file
    * this interface is implemented to make sure the application could run correctly
    * @return either SparkJobValid or SparkJobInvalid
    */
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    // check datasourceStringList()
    Try(config.getString(datasourceString))
      .filter(s => s.startsWith("hdfs") || Files.exists(Paths.get(s)))
      .map(m => SparkJobValid)
      .getOrElse(SparkJobInvalid(s"data source not exists"))

    // check etlconfig
    Try(config.getConfig(etlConfigString)).map(
      m => (m.getIntList(extractConfigString),
        m.getObjectList(transformConfigString),
        m.getObjectList(loadConfigString))
    ).map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("etlconfig error"))
  }
}
