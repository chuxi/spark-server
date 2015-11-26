package models

import play.api.data.Form
import play.api.data.Forms._

/**
  * Created by king on 15-11-26.
  */
case class SubmitJobForm(appName: String, classPath: String, config: String,
                         contextOpt: Option[String], syncOpt: Option[Boolean], timeoutOpt: Option[Int])

object SubmitJobForm {
  val submitJobForm = Form(
    mapping(
      "appName" -> nonEmptyText,
      "classPath" -> nonEmptyText,
      "config" -> text(minLength = 2),
      "contextOpt" -> optional(text),
      "syncOpt" -> optional(boolean),
      "timeoutOpt" -> optional(number)
    )(SubmitJobForm.apply)(SubmitJobForm.unapply)
  )
}
