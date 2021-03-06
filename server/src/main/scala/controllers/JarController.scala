package controllers

import java.nio.file.Files
import java.util.Date

import akka.actor.ActorRef
import akka.util.Timeout
import com.google.inject.name.Named
import com.google.inject.{Inject, Singleton}
import models.AppInfo
import org.apache.logging.log4j.LogManager
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.concurrent.duration._
import akka.pattern.ask
import services._

/**
  * Created by king on 15-11-16.
  */
@Singleton
class JarController @Inject() (@Named("jar-actor") jarManager: ActorRef) extends Controller {
  import services.JarManager._

  implicit val timeout = Timeout(5.seconds)
  private val logger = LogManager.getLogger(getClass)

  logger.info("JarController is initialized.")

  def listJars() = Action.async {
    (jarManager ? ListJars).mapTo[collection.Map[String, Date]].map{ jarTimeMap =>
      val stringTimeMap = jarTimeMap.map(m => AppInfo(m._1, m._2)).toList
      Ok(Json.toJson(stringTimeMap))
    } recover {
      case e: Exception => BadRequest(e.toString)
    }
  }

  def postJar(appName: String) = Action.async { implicit request =>
    val jarBytes = request.body.asMultipartFormData.map{ data =>
      data.file("jar").map { jar =>
        Files.readAllBytes(jar.ref.file.toPath)
      }.get
    }.get

//    val jarBytes = request.body.asBytes().get
    val future = jarManager ? StoreJar(appName, jarBytes)
    future.map {
      case JarStored => Ok
      case InvalidJar => BadRequest("Jar is not of the right format")
    }.recover {
      case e: Exception => InternalServerError(e.toString)
    }
  }

}