package cn.edu.zju.king.jobserver.etl

import cn.edu.zju.king.serverapi._
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, Config}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, Logging, SparkContext}

import scala.util.Try

/**
  * Created by king on 15-12-14.
  * source {
  *   type: "file"
  *   file {
  *     url: "hdfs:///samples.parquet"
  *   }
  * }
  * sharedrdds: "source"
  *
  * etl {
  *   ...
  * }
  *
  */
object DataSourceApp extends SparkJob with NamedRddSupport with Logging {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    // split the row by delimiter comma symbol.
    val data: RDD[Array[String]] = sc.textFile(jobConfig.getString("source.file.url"))
      .map(_.split(",").map(_.trim))
    namedRdds.update(jobConfig.getString("sharedrdds"), data)
  }

  override def validate(sc: SparkContext, jobConfig: Config): SparkJobValidation = {
    lazy val error = SparkJobInvalid("spark job config error " + jobConfig.getString("source.file.url"))
    Try(jobConfig.getConfig("source")).map(x => {
//      x.getString("type")
      x.getString("file.url")
    }).map(x => SparkJobValid)
      .getOrElse(error) &&
    Try(jobConfig.getString("sharedrdds")).map(x => SparkJobValid).getOrElse(error)
  }
}
