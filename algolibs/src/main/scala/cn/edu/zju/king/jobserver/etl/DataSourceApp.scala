package cn.edu.zju.king.jobserver.etl

import java.nio.file.{Paths, Files}

import cn.edu.zju.king.serverapi._
import com.typesafe.config.Config
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SQLContext

import scala.util.Try

/**
  * Created by king on 15-11-20.
  */
object DataSourceApp extends SparkSqlJob {
  private val logger = LogManager.getLogger(getClass)
  // should be hdfs:// or file:// protocal file
  private val datasourceString = "datasource"

  def main(args: Array[String]) {

  }

  override def runJob(sc: SQLContext, jobConfig: Config): Any = {
    val datasource = jobConfig.getString(datasourceString)
    sc.read.load()
    println(sc.createExternalTable("templateTable", datasource).count())
  }

  override def validate(sc: SQLContext, config: Config): SparkJobValidation = {
    // check datasourceStringList()
    Try(config.getString(datasourceString))
      .filter(s => s.startsWith("hdfs") || Files.exists(Paths.get(s)))
      .map(m => SparkJobValid)
      .getOrElse(SparkJobInvalid(s"data source not exists"))
  }

}
