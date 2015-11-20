package services.contexts

import cn.edu.zju.king.serverapi.{ContextLike, SparkJob, SparkJobBase}
import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import services.util.SparkJobUtils

/**
 * Created by king on 15-9-22.
 */
trait SparkContextFactory {

  type C <: ContextLike

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C

  def makeContext(config: Config, contextConfig: Config, contextName: String): C = {
    val sparkConf = SparkJobUtils.configToSparkConf(config, contextConfig, contextName)
    val contextCfg = config.getConfig("spark.context-settings").withFallback(contextConfig)
    makeContext(sparkConf, contextCfg, contextName)
  }
}

class DefaultSparkContextFactory extends SparkContextFactory {
  type C = SparkContext with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    new SparkContext(sparkConf) with ContextLike {
      def sparkContext: SparkContext = this
      def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SparkJob]
    }
  }
}