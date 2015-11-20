package modules

import com.google.inject.AbstractModule
import org.apache.logging.log4j.LogManager
import play.api.{Environment, Configuration}
import services.io.JobDAO

/**
  * Created by king on 15-11-16.
  */
class JobDaoModules(environment: Environment,
                    configuration: Configuration) extends AbstractModule {
  private val logger = LogManager.getLogger(getClass)

  override def configure(): Unit = {
    val daoClassName = configuration.getString("spark.jobserver.jobdao").get
    val daoClass: Class[_ <: JobDAO] = environment.classLoader.loadClass(daoClassName)
      .asSubclass(classOf[JobDAO])
    bind(classOf[JobDAO]).to(daoClass)
    logger.info(s"$daoClassName is loaded successfully!")
  }
}
