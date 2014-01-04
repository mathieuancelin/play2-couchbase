package controllers

import play.api.mvc._
import play.api.Play.current
import org.ancelin.play2.couchbase.{Couchbase, CouchbaseController}
import play.api.libs.json.JsObject
import models.User
import models.User.userFmt
import org.ancelin.play2.couchbase.plugins.CouchbaseN1QLPlugin._

object Application extends Controller with CouchbaseController {

  implicit val couchbaseExecutionContext = Couchbase.couchbaseExecutor

  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  // Usage of CouchbaseAction
  def getUser(key: String) = CouchbaseAction("default") { bucket =>
    bucket.get[JsObject](key).map { maybeUser =>
      maybeUser
        .map(user => Ok(user))
        .getOrElse(BadRequest(s"Unable to find user with key: $key"))
    }
  }

  // Usage of N1QL plugin
  def findUserByEmail(email: String) = Action.async {
    N1QL( s""" SELECT id, name, email FROM default WHERE email = '${email}' """ ).toList[User].map { users =>
      Ok(users.mkString(" | "))
    }
  }
}

