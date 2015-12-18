package services.contexts

import cn.edu.zju.king.serverapi.{SparkJob, SparkJobBase}
import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import services.util.SparkJobUtils

/**
 * Created by king on 15-9-22.
 */
trait SparkContextFactory {

  type C <: ContextLike

  def makeContext(sparkConf: SparkConf): C

  def makeContext(sparkMaster: String, contextName: String, contextConfig: Config): C = {
    val sparkConf = SparkJobUtils.configToSparkConf(sparkMaster, contextName, contextConfig)
//    val contextCfg = config.getConfig("spark.context-settings").withFallback(contextConfig)
    makeContext(sparkConf)
  }
}

class DefaultSparkContextFactory extends SparkContextFactory {
  type C = SparkContext with ContextLike

  def makeContext(sparkConf: SparkConf): C = {
    new SparkContext(sparkConf) with ContextLike {
      def sparkContext: SparkContext = this
      def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SparkJob]
    }
  }
}