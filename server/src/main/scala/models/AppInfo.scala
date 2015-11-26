package models

import java.util.Date

import play.api.libs.json.{Json, JsValue, Writes}

/**
  * Created by king on 15-11-24.
  */

case class AppInfo(appName: String, uploadedDate: Date)

object AppInfo {
  implicit val appInfoWrites = new Writes[AppInfo] {
    override def writes(o: AppInfo): JsValue = Json.obj(
      "appName" -> o.appName,
      "uploadedDate" -> o.uploadedDate
    )
  }
}
