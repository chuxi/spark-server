package services.util

import com.typesafe.config.{ConfigException, Config}
import org.apache.logging.log4j.LogManager

import scala.reflect.runtime.{universe => ru}

/**
 * Created by king on 15-9-22.
 */
trait SparkMasterProvider {

  def getSparkMaster(config: Config): String
}

object SparkMasterProvider {
  private val logger = LogManager.getLogger(getClass)
  val sparkMasterProvider = "spark.master.provider"

  /**
   * Will look for an Object with the name provided in the Config file and return it
   * or the DefaultSparkMasterProvider if no spark.master.provider was specified
   * @param config SparkJobserver Config
   * @return A SparkMasterProvider
   */
  def fromConfig(config: Config): SparkMasterProvider = {
    try {
      val sparkMasterObjectName = config.getString(sparkMasterProvider)
      logger.info(s"Using $sparkMasterObjectName to determine Spark Master")
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val module = m.staticModule(sparkMasterObjectName)
      val sparkMasterProviderObject = m.reflectModule(module).instance.asInstanceOf[SparkMasterProvider]
      sparkMasterProviderObject
    } catch {
      case me: ConfigException => DefaultSparkMasterProvider
      case e: Exception => throw e
    }
  }
}

object DefaultSparkMasterProvider extends SparkMasterProvider {
  def getSparkMaster(config: Config): String = config.getString("spark.master")
}
