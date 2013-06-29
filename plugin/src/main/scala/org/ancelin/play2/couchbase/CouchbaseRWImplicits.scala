package org.ancelin.play2.couchbase

import play.api.libs.json._
import play.api.libs.json.JsSuccess
import play.api.libs.json.JsObject

object CouchbaseRWImplicits {
  implicit val documentAsStringReader = new Reads[String] {
    def reads(json: JsValue): JsResult[String] = JsSuccess(Json.stringify(json))
  }
  implicit val documentAsJsObjectReader = new Reads[JsObject] {
    def reads(json: JsValue): JsResult[JsObject] = JsSuccess(json.as[JsObject])
  }
  implicit val stringToDocumentWriter = new Writes[String] {
    def writes(o: String): JsValue = Json.parse(o)
  }
  implicit val jsObjectToDocumentWriter = new Writes[JsObject] {
    def writes(o: JsObject): JsValue = o
  }
}
