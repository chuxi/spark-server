package modules

import com.google.inject.AbstractModule
import org.apache.logging.log4j.LogManager
import play.api.libs.concurrent.AkkaGuiceSupport
import services.{JobInfoManager, ContextManager, JarManager}

/**
  * Created by king on 15-11-16.
  */
class ActorModules extends AbstractModule with AkkaGuiceSupport {
  private val logger = LogManager.getLogger(getClass)

  override def configure(): Unit = {
    bindActor[JarManager]("jar-actor")
    logger.info("Bind JarManager actor to jar-actor successfully!")
    bindActor[ContextManager]("context-actor")
    logger.info("Bind ContextManager actor to context-actor successfully!")
    bindActor[JobInfoManager]("jobinfo-actor")
    logger.info("Bind JobInfoManager actor to jobinfo-actor successfully!")
  }
}
