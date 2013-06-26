package org.ancelin.play2.couchbase

import play.api.libs.json._

object CouchbaseWrites {
  implicit val stringToDocumentWriter = new Writes[String] {
    def writes(o: String): JsValue = {
      Json.parse(o)
    }
  }

  implicit val jsObjectToDocumentWriter = new Writes[JsObject] {
    def writes(o: JsObject): JsValue = o
  }
}
