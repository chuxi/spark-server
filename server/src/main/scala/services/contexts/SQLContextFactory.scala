package services.contexts

import cn.edu.zju.king.serverapi.{ContextLike, SparkJobBase, SparkSqlJob}
import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by king on 15-10-20.
 */
class SQLContextFactory extends SparkContextFactory {
  type C = SQLContext with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config,  contextName: String): C = {
    new SQLContext(new SparkContext(sparkConf)) with ContextLike {
      def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SparkSqlJob]
      def stop() { this.sparkContext.stop() }
    }
  }
}
