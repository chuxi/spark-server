package services.util

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import org.apache.spark.SparkConf

import scala.util.Try

/**
 * Created by king on 15-9-21.
 */
object SparkJobUtils {
  import collection.JavaConverters._

  def configToSparkConf(sparkMaster: String, contextName: String, contextConfig: Config): SparkConf = {
//    val sparkMaster = SparkMasterProvider.fromConfig(config).getSparkMaster(config)

    val conf = new SparkConf()
    conf.setMaster(sparkMaster).setAppName(contextName)

    for (cores <- Try(contextConfig.getInt("num-cpu-cores"))) {
      conf.set("spark.cores.max", cores.toString)
    }

    for (nodeMemStr <- Try(contextConfig.getString("memory-per-node"))) {
      conf.set("spark.executor.memory", nodeMemStr)
    }

//    Try(config.getString("spark.home")).foreach { home => conf.setSparkHome(home) }

    conf.set("spark.ui.port", "0")

//    if (sparkMaster == "yarn-client") {
//      conf.set("spark.broadcast.factory", config.getString("spark.jobserver.yarn-broadcast-factory"))
//    }

    for (e <- contextConfig.entrySet().asScala if e.getKey.startsWith("spark.")) {
      conf.set(e.getKey, e.getValue.unwrapped.toString)
    }

    for (e <- Try(contextConfig.getConfig("passthrough"))) {
      e.entrySet().asScala.map { s=>
        conf.set(s.getKey, s.getValue.unwrapped.toString)
      }
    }

    conf
  }

  /**
   * Returns the maximum number of jobs that can run at the same time
   */
  def getMaxRunningJobs(config: Config): Int = {
    val cpuCores = Runtime.getRuntime.availableProcessors
    Try(config.getInt("spark.server.max-jobs-per-context")).getOrElse(cpuCores)
  }

  def getContextTimeout(config: Config): Int = {
    config.getString("spark.master") match {
      case "yarn-client" =>
        Try(config.getDuration("spark.server.yarn-context-creation-timeout", TimeUnit.MILLISECONDS).toInt / 1000).getOrElse(40)
      case _  =>
        Try(config.getDuration("spark.server.context-creation-timeout", TimeUnit.MILLISECONDS).toInt / 1000).getOrElse(15)
    }
  }

}
