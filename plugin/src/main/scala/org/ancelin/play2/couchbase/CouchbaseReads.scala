package org.ancelin.play2.couchbase

import play.api.libs.json._
import play.api.libs.json.JsSuccess

object CouchbaseReads {
  implicit val documentAsStringReader = new Reads[String] {
    def reads(json: JsValue): JsResult[String] = JsSuccess(Json.stringify(json))
  }
}
