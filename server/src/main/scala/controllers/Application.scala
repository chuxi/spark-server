package controllers

import javax.inject.{Inject, Singleton}

import org.apache.logging.log4j.LogManager
import play.api.mvc.{Action, Controller}

/**
  * Created by king on 15-11-15.
  * for handling the index page and reroute for angularjs
  */
@Singleton
class Application @Inject()() extends Controller {
  private val logger = LogManager.getLogger(getClass)

  logger.info("Application Controller is initialized.")

  def index(any: String) = Action {
    Ok(html.index())
  }
}
