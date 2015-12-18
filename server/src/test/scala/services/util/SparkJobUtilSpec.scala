package services.util

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.scalatest.{FunSpec, Matchers}

/**
 * Created by king on 15-10-8.
 */
class SparkJobUtilSpec extends FunSpec with Matchers {
  import collection.JavaConverters._

  val config = ConfigFactory.parseMap(Map(
    "spark.home" -> "/usr/local/spark",
    "spark.master" -> "local[4]"
  ).asJava)
  val contextName = "demo"

  def getSparkConf(configMap: Map[String, AnyRef]): SparkConf =
    SparkJobUtils.configToSparkConf(config.getString("spark.master"), contextName, ConfigFactory.parseMap(configMap.asJava))

  describe("SparkJobUtils.configToSparkConf") {
    it("should translate num-cpu-cores and memory-per-node properly") {
      val sparkConf = getSparkConf(Map("num-cpu-cores" -> "4", "memory-per-node" -> "512m"))
      sparkConf.get("spark.master") should equal ("local[4]")
      sparkConf.get("spark.cores.max") should equal ("4")
      sparkConf.get("spark.executor.memory") should equal ("512m")
//      sparkConf.get("spark.home") should equal ("/usr/local/spark")
    }

    it("should add other arbitrary settings by following passthrough") {
      val sparkConf = getSparkConf(Map("passthrough.spark.cleaner.ttl" -> "86400"))
      sparkConf.getInt("spark.cleaner.ttl", 0) should equal (86400)
    }
  }

}
