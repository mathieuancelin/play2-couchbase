package models

import org.ancelin.play2.couchbase.Couchbase
import scala.concurrent.Future
import play.api.libs.json.{Json, JsObject}
import play.api.Play.current
import org.ancelin.play2.couchbase.CouchbaseRWImplicits.documentAsJsObjectReader
import org.ancelin.play2.couchbase.CouchbaseRWImplicits.jsObjectToDocumentWriter
import com.couchbase.client.protocol.views.{ComplexKey, Stale, Query}
import net.spy.memcached.ops.OperationStatus

case class User(id: String, name: String, email: String)

// classic usage of bucket for an entity
object User {

  implicit val ec = Couchbase.couchbaseExecutor
  implicit val userFmt = Json.format[User]

  def bucket = Couchbase.bucket("default")

  def findById(id: String): Future[Option[JsObject]] = {
    bucket.get[JsObject](id)
  }

  def findAll(): Future[List[JsObject]] = {
    bucket.find[JsObject]("users", "by_name")(new Query().setIncludeDocs(true).setStale(Stale.FALSE))
  }

  def findByName(name: String): Future[Option[JsObject]] = {
    val query = new Query().setIncludeDocs(true).setLimit(1)
      .setRangeStart(ComplexKey.of(name))
      .setRangeEnd(ComplexKey.of(s"$name\uefff")).setStale(Stale.FALSE)
    bucket.find[JsObject]("users", "by_name")(query).map(_.headOption)
  }

  def findByEmail(email: String): Future[Option[JsObject]] = {
    val query = new Query().setIncludeDocs(true).setLimit(1)
      .setRangeStart(ComplexKey.of(email))
      .setRangeEnd(ComplexKey.of(s"$email\uefff")).setStale(Stale.FALSE)
    bucket.find[JsObject]("users", "by_email")(query).map(_.headOption)
  }

  def save(user: JsObject): Future[OperationStatus] = {
    bucket.set[JsObject]((user \ "id").as[String], user)
  }

  def remove(user: JsObject): Future[OperationStatus] = {
    bucket.delete((user \ "id").as[String])
  }
}