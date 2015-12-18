package cn.edu.zju.king.jobserver.etl

import java.nio.file.{Files, Paths}

import cn.edu.zju.king.serverapi.{SparkJobValid, SparkJobInvalid, SparkJob, SparkJobValidation}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.logging.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.util.Try

/**
  * Created by king on 15-11-19.
  */
object ExtractionApp extends SparkJob {
//  private val logger = LogManager.getLogger(getClass)
  val configString =
    """
      | source {
      |   type: "file"
      |   file {
      |     url: "hdfs:///sample.txt"
      |   }
      | }
      |
      | sharedrdds: "source"
      |
      | etl {
      |   extract {
      |     table: "temptable"
      |     schema: "col1, col2, col3..."
      |     sql: "select ... from temptable where ..."
      |   }
      |
      |   transform {}
      |
      |   load {
      |     type: "file"
      |     url: "hdfs:///result.parquet"
      |   }
      | }
      |
    """.stripMargin


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://192.168.1.11:7077").setAppName("DataLoad")
//    conf.set("fs.defaultFS", "hdfs://192.168.1.11:9000")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString(configString)
    runJob(sc, config)
  }

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
//    val sqlc = new SQLContext(sc)
    val data = sc.textFile("hdfs://192.168.1.11:9000/sample.txt", minPartitions = 4)
    data.collect().foreach(println)
  }

  /**
    * validate the config file
    * this interface is implemented to make sure the application could run correctly
    * @return either SparkJobValid or SparkJobInvalid
    */
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}
