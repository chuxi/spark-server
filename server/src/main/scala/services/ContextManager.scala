package services

import akka.actor._
import akka.util.Timeout
import akka.pattern.ask
import com.google.inject.{Inject, Singleton}
import com.typesafe.config.Config
import org.apache.logging.log4j.LogManager
import play.api.Configuration
import services.actors.JobManagerActor._
import services.actors.{JobManagerActor, JobResultActor}
import services.io.JobDAO
import services.util.SparkJobUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by king on 15-11-17.
  */
object ContextManager {
  // Messages/actions
  case object AddContextsFromConfig // Start up initial contexts
  case object ListContexts
  case class AddContext(name: String, contextConfig: Config)
  case class GetAdHocContext(classPath: String, contextConfig: Config)
  case class GetContext(name: String) // returns JobManager, JobResultActor
  case class GetResultActor(name: String)  // returns JobResultActor
  case class StopContext(name: String)

  // Errors/Responses
  case object ContextInitialized
  case class ContextInitError(t: Throwable)
  case object ContextAlreadyExists
  case object NoSuchContext
  case object ContextStopped
}


@Singleton
class ContextManager @Inject() (jobDao: JobDAO) extends Actor {
  import ContextManager._
  private val logger = LogManager.getLogger(getClass)

  val config = context.system.settings.config
  val defaultContextConfig = config.getConfig("spark.context-settings")
  val contextTimeout = SparkJobUtils.getContextTimeout(config)

  private val contexts = mutable.HashMap.empty[String, ActorRef]
  private val resultActors = mutable.HashMap.empty[String, ActorRef]
  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  val globalResultActor = context.actorOf(Props[JobResultActor], "global-result-actor")

  override def receive: Receive = {
    // not used
    case AddContextsFromConfig =>
      addContextsFromConfig(config)

    case ListContexts =>
      sender ! contexts.keys.toSeq

    case AddContext(name, contextConfig) =>
      logger.info(s"Creating SparkContext $name for jobs.")
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      if (contexts contains name) {
        originator ! ContextAlreadyExists
      } else {
        startContext(name, mergedConfig, isAdHoc = false, contextTimeout) { contextMgr =>
          originator ! ContextInitialized
        } { err =>
          originator ! ContextInitError(err)
        }
      }

      // now I do not want to have AdHocContext, it leads to un-managed contexts
    case GetAdHocContext(classPath, contextConfig) =>
      logger.info("Creating SparkContext for adhoc jobs.")
      val originator = sender() // Sender is a mutable reference, must capture in immutable val
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)

      // Keep generating context name till there is no collision
      var contextName = ""
      do {
        contextName = java.util.UUID.randomUUID().toString.substring(0, 8) + "-" + classPath
      } while (contexts contains contextName)

      // Create JobManagerActor and JobResultActor
      startContext(contextName, mergedConfig, isAdHoc = true, contextTimeout) { contextMgr =>
        originator ! (contexts(contextName), resultActors(contextName))
      } { err =>
        originator ! ContextInitError(err)
      }

    case GetResultActor(name) =>
      sender ! resultActors.getOrElse(name, globalResultActor)

    case GetContext(name) =>
      if (contexts contains name) {
        val future = (contexts(name) ? SparkContextStatus) (contextTimeout.seconds)
        val originator = sender()
        future.collect {
          case SparkContextAlive => originator ! (contexts(name), resultActors(name))
          case SparkContextDead =>
            logger.info(s"SparkContext $name is dead")
            self ! StopContext(name)
            originator ! NoSuchContext
        }
      } else {
        sender ! NoSuchContext
      }

    case StopContext(name) =>
      if (contexts contains name) {
        logger.info(s"Shutting down context $name")
        // watch for the Terminated Message
        context.watch(contexts(name))
        contexts(name) ! PoisonPill
        resultActors.remove(name)
        sender ! ContextStopped
      } else {
        sender ! NoSuchContext
      }

    case Terminated(actorRef) =>
      val name: String = actorRef.path.name
      logger.info("Actor terminated: " + name)
      contexts.remove(name)
  }

  private def startContext(name: String, contextConfig: Config, isAdHoc: Boolean, timeoutSecs: Int = 1)
                          (successFunc: ActorRef => Unit)
                          (failureFunc: Throwable => Unit): Unit = {
    require(!contexts.contains(name), "There is already a context named " + name)
    logger.info(s"Creating a context named $name")

    val resultActorRef = if (isAdHoc) Some(globalResultActor) else None
    val ref = context.actorOf(Props(classOf[JobManagerActor], jobDao, name, contextConfig, isAdHoc, resultActorRef), name)
    (ref ? Initialize)(Timeout(timeoutSecs.seconds)).onComplete {
      case Failure(e: Exception) =>
        logger.error(s"Exception after sending Initialize to JobManagerActor", e)
        // Make sure we try to shut down the context in case it gets created anyways
        ref ! PoisonPill
        failureFunc(e)
      case Success(Initialized(resultActor)) =>
        logger.info(s"SparkContext $name initialized")
        contexts(name) = ref
        resultActors(name) = resultActor
        successFunc(ref)
      case Success(InitError(t)) =>
        ref ! PoisonPill
        failureFunc(t)
      case x =>
        logger.warn(s"Unexpected message received by startContext: $x")
    }
  }

  private def addContextsFromConfig(config: Config): Unit = {
    for (contexts <- Try(config.getObject("spark.contexts"))) {
      contexts.keySet().asScala.foreach { contextName =>
        val contextConfig = config.getConfig("spark.contexts." + contextName)
          .withFallback(defaultContextConfig)
        startContext(contextName, contextConfig, isAdHoc = false, contextTimeout) { ref => } {
          e => logger.error("Unable to start context " + contextName, e)
        }
        Thread sleep 500 // Give some spacing so multiple contexts can be created
      }
    }
  }
}
