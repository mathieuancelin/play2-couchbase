package models

import play.api.libs.json.Json
import org.ancelin.play2.couchbase.Couchbase
import org.ancelin.play2.couchbase.Couchbase._
import com.couchbase.client.protocol.views.{ComplexKey, Query}
import net.spy.memcached.ops.OperationStatus
import scala.concurrent.Future
import play.api.Play.current
import play.api.data.Form
import play.api.data._
import play.api.data.Forms._
import play.api.data.validation.Constraints._
import net.spy.memcached.PersistTo

case class People(id: Option[String], name: String, surname: String)

object Peoples {

  implicit val peopleReader = Json.reads[People]
  implicit val peopleWriter = Json.writes[People]
  implicit val ec = Couchbase.couchbaseExecutor
  implicit val client = Couchbase.client("people")

  val peopleForm: Form[People] = Form(
    mapping(
      "id" -> optional(text),
      "name" -> nonEmptyText,
      "surname" -> nonEmptyText
    )(People.apply)(People.unapply)
  )

  def findById(id: String): Future[Option[People]] = {
    get[People](id)
  }

  def findAll(): Future[List[People]] = {
    find[People]("people", "by_name")(new Query().setIncludeDocs(true))
  }

  def findByName(name: String): Future[Option[People]] = {
    val query = new Query().setIncludeDocs(true).setLimit(1)
      .setRangeStart(ComplexKey.of(name))
      .setRangeEnd(ComplexKey.of(s"$name\uefff"))
    find[People]("people", "by_name")(query).map(_.headOption)
  }

  def save(people: People): Future[OperationStatus] = {
    set[People]( people.id.get, people, PersistTo.ONE )
  }

  def remove(id: String): Future[OperationStatus] = {
    delete(id)
  }
}