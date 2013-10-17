package controllers


import org.ancelin.play2.couchbase.crud.CouchbaseCrudSourceController
import models.User
import models.User.userFmt
import org.ancelin.play2.couchbase.Couchbase
import play.api.Play.current

object UsersController extends CouchbaseCrudSourceController[User] {
  def bucket = Couchbase.bucket("default")
  override def defaultViewName = "by_name"
  override def defaultDesignDocname = "users"
  override def idKey = "id"
}