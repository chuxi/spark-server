package controllers

import akka.actor.ActorRef
import akka.util.Timeout
import com.google.inject.name.Named
import com.google.inject.{Inject, Singleton}
import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.LogManager
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}
import services.ContextManager._
import scala.concurrent.duration._
import akka.pattern.ask
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by king on 15-11-17.
  */
@Singleton
class ContextController @Inject() (@Named("context-actor") contextManager: ActorRef) extends Controller{
  import collection.JavaConverters._

  implicit val timeout = Timeout(5.seconds)
  private val logger = LogManager.getLogger(getClass)

  logger.info("ContextController is initialized.")

  def listContexts() = Action.async {
    (contextManager ? ListContexts).mapTo[Seq[String]].map{ contexts =>
      Ok(Json.toJson(contexts))
    }
  }

  /**
    *  POST /contexts/<contextName>?<optional params> -
    *    Creates a long-running context with contextName and options for context creation
    *    All options are merged into the defaults in spark.context-settings
    *
    * optional @param {Int} [num-cpu-cores] - Number of cores the context will use
    * optional @param {String} [memory-per-node] - -Xmx style string (512m, 1g, etc)
    * for max memory per node
    * @return the string "OK", or error if context exists or could not be initialized
    */
  def createContext(contextName: String) = Action.async { implicit request =>
    val params = request.queryString.map{ m => m._1 -> m._2.head }
    logger.info(s"params $params")
    val config = ConfigFactory.parseMap(params.asJava).resolve()
    (contextManager ? AddContext(contextName, config)).map{
      case ContextAlreadyExists => BadRequest(s"context $contextName already exists")
      case ContextInitialized => Ok
      case ContextInitError(e) => InternalServerError(s"context $contextName initial error by $e")
    }
  }

  def stopContext(contextName: String) = Action.async {
    (contextManager ? StopContext(contextName)).map{
      case ContextStopped => Ok
      case NoSuchContext => NotFound(s"context $contextName not found")
    }
  }


}
