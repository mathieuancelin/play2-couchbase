package models

import org.ancelin.play2.couchbase.Couchbase
import scala.concurrent.Future
import play.api.libs.json.JsObject
import play.api.Play.current
import org.ancelin.play2.couchbase.CouchbaseRWImplicits.documentAsJsObjectReader
import org.ancelin.play2.couchbase.CouchbaseRWImplicits.jsObjectToDocumentWriter
import com.couchbase.client.protocol.views.{ComplexKey, Stale, Query}
import net.spy.memcached.ops.OperationStatus

object User {

  implicit val ec = Couchbase.couchbaseExecutor

  def bucket = Couchbase.bucket("default")

  def findById(id: String): Future[Option[JsObject]] = {
    bucket.get[JsObject](id)
  }

  def findAll(): Future[List[JsObject]] = {
    bucket.find[JsObject]("user", "by_name")(new Query().setIncludeDocs(true).setStale(Stale.FALSE))
  }

  def findByName(name: String): Future[Option[JsObject]] = {
    val query = new Query().setIncludeDocs(true).setLimit(1)
      .setRangeStart(ComplexKey.of(name))
      .setRangeEnd(ComplexKey.of(s"$name\uefff")).setStale(Stale.FALSE)
    bucket.find[JsObject]("user", "by_name")(query).map(_.headOption)
  }

  def save(user: JsObject): Future[OperationStatus] = {
    bucket.set[JsObject]((user \ "id").as[String], user)
  }

  def remove(user: JsObject): Future[OperationStatus] = {
    bucket.delete((user \ "id").as[String])
  }
}