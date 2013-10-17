package controllers

import play.api.mvc.Controller
import org.ancelin.play2.couchbase.{Couchbase, CouchbaseController}
import org.ancelin.play2.couchbase.CouchbaseRWImplicits.documentAsJsObjectReader
import play.api.libs.json.JsObject
import play.api.Play.current

object MyCouchbaseController extends Controller with CouchbaseController {

  implicit val couchbaseExecutionContext = Couchbase.couchbaseExecutor

  def getUser(key: String) = CouchbaseAction { bucket =>
    bucket.get[JsObject](key).map { maybeUser =>
      maybeUser
        .map(user => Ok(user))
        .getOrElse(BadRequest(s"Unable to find user with key: $key"))
    }
  }
}

