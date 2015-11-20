package services.util

import play.api.libs.json.{Json, JsValue, Writes}

/**
  * Created by king on 15-11-18.
  */
object JsonUtils {

  implicit val jsonMapWrites = new Writes[Map[String, Any]] {
    override def writes(o: Map[String, Any]): JsValue = Json.obj(

    )
  }
}
