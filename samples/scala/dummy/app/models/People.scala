package models

import play.api.libs.json.Json
import org.ancelin.play2.couchbase.Couchbase
import com.couchbase.client.protocol.views.{Stale, ComplexKey, Query}
import net.spy.memcached.ops.OperationStatus
import scala.concurrent.Future
import play.api.Play.current
import play.api.data._
import play.api.data.Forms._
import net.spy.memcached.PersistTo

case class People(id: Option[String], name: String, surname: String)

object Peoples {

  implicit val peopleReader = Json.reads[People]
  implicit val peopleWriter = Json.writes[People]
  implicit val ec = Couchbase.couchbaseExecutor

  def bucket = Couchbase.bucket("people")

  val peopleForm: Form[People] = Form(
    mapping(
      "id" -> optional(text),
      "name" -> nonEmptyText,
      "surname" -> nonEmptyText
    )(People.apply)(People.unapply)
  )

  def findById(id: String): Future[Option[People]] = {
    bucket.get[People](id)
  }

  def findAll(): Future[List[People]] = {
    bucket.find[People]("people", "by_name")(new Query().setIncludeDocs(true).setStale(Stale.FALSE))
  }

  def findByName(name: String): Future[Option[People]] = {
    val query = new Query().setIncludeDocs(true).setLimit(1)
      .setRangeStart(ComplexKey.of(name))
      .setRangeEnd(ComplexKey.of(s"$name\uefff"))
    bucket.find[People]("people", "by_name")(query).map(_.headOption)
  }

  def save(people: People): Future[OperationStatus] = {
    bucket.set[People]( people.id.get, people )
  }

  def remove(id: String): Future[OperationStatus] = {
    bucket.delete(id)
  }
}