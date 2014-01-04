package controllers


import org.ancelin.play2.couchbase.crud.CouchbaseCrudSourceController
import models.User
import models.User.userFmt
import org.ancelin.play2.couchbase.Couchbase
import play.api.Play.current
import play.api._
import play.api.mvc._

object UsersController extends CouchbaseCrudSourceController[User] {
  val viewPrefix = if (play.api.Play.isDev) "dev_" else ""
  def bucket = Couchbase.bucket("default")
  override def defaultViewName = "by_name"
  override def defaultDesignDocname = s"${viewPrefix}users"

  def index = Action {
    Ok(views.html.index())
  }
}