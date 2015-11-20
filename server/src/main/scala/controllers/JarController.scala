package controllers

import java.util.Date

import akka.actor.ActorRef
import akka.util.Timeout
import com.google.inject.name.Named
import com.google.inject.{Inject, Singleton}
import org.apache.logging.log4j.LogManager
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.concurrent.duration._
import akka.pattern.ask

/**
  * Created by king on 15-11-16.
  */
@Singleton
class JarController @Inject() (@Named("jar-actor") jarManager: ActorRef) extends Controller {
  import services.JarManagerMessages._

  implicit val timeout = Timeout(5.seconds)
  private val logger = LogManager.getLogger(getClass)

  logger.info("JarController is initialized.")

  def listJars() = Action.async {
    (jarManager ? ListJars).mapTo[collection.Map[String, Date]].map{ jarTimeMap =>
      val stringTimeMap = jarTimeMap.map(m => (m._1, m._2.getTime.toString)).toMap
      Ok(Json.toJson(stringTimeMap))
    } recover {
      case e: Exception => BadRequest(e.toString)
    }
  }

  def postJar(appName: String) = Action.async(parse.raw) { implicit request =>
    val jarBytes = request.body.asBytes().get
    val future = jarManager ? StoreJar(appName, jarBytes)
    future.map{
      case JarStored => Ok
      case InvalidJar => BadRequest("Jar is not of the right format")
    }.recover {
      case e: Exception => InternalServerError(e.toString)
    }
  }


}