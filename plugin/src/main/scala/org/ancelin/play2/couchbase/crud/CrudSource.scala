package org.ancelin.play2.couchbase.crud

import scala.concurrent.{Future, ExecutionContext}
import play.api.libs.json._
import com.couchbase.client.protocol.views.{ComplexKey, Stale, Query, View}
import play.api.libs.iteratee.{Iteratee, Enumerator}
import play.api.mvc.{Action, EssentialAction, Controller}
import org.ancelin.play2.couchbase.{Couchbase, CouchbaseBucket}
import java.util.UUID
import net.spy.memcached.ops.OperationStatus

class CouchbaseCrudSource[T:Format](bucket: CouchbaseBucket) {

  import org.ancelin.play2.couchbase.CouchbaseImplicitConversion.Couchbase2ClientWrapper
  import play.api.Play.current

  val reader: Reads[T] = implicitly[Reads[T]]
  val writer: Writes[T] = implicitly[Writes[T]]
  val ctx: ExecutionContext = Couchbase.couchbaseExecutor

  def insert(t: T): Future[String] = {
    val id = UUID.randomUUID().toString
    bucket.set(id, t)(bucket, writer, ctx).map(_ => id)(ctx)
  }

  def get(id: String): Future[Option[(T, String)]] = {
    bucket.get[T]( id )(bucket ,reader, ctx).map( _.map( v => ( v, id ) ) )(ctx)
  }

  def delete(id: String): Future[OperationStatus] = {
    bucket.delete(id)(bucket, ctx)
  }

  def update(id: String, t: T): Future[OperationStatus] = {
    bucket.replace(id, t)(bucket, writer, ctx)
  }

  def find(view: View, query: Query): Future[List[T]] = {
    bucket.find[T](view)(query)(bucket, reader, ctx)
  }

  def findStream(view: View, query: Query): Future[Enumerator[T]] = {
    bucket.findAsEnumerator[T](view)(query)(bucket, reader, ctx)
  }
}

abstract class CouchbaseCrudSourceController[T:Format] extends Controller {

  import org.ancelin.play2.couchbase.CouchbaseImplicitConversion.Couchbase2ClientWrapper
  import play.api.Play.current

  val bucket: CouchbaseBucket

  lazy val res = new CouchbaseCrudSource[T](bucket)

  implicit val ctx = Couchbase.couchbaseExecutor

  val writerWithId = Writes[(T, String)] {
    case (t, id) =>
      Json.obj("id" -> id) ++
        res.writer.writes(t).as[JsObject]
  }

  def insert = Action(parse.json){ request =>
    Json.fromJson[T](request.body)(res.reader).map{ t =>
      Async{
        res.insert(t).map{ id => Ok(Json.obj("id" -> id)) }
      }
    }.recoverTotal{ e => BadRequest(JsError.toFlatJson(e)) }
  }

  def get(id: String) = Action {
    Async{
      res.get(id).map{
        case None    => NotFound(s"ID ${id} not found")
        case Some(tid) => Ok(Json.obj("id" -> id))
      }
    }
  }

  def delete(id: String) = Action {
    Async{
      res.delete(id).map{ le => Ok(Json.obj("id" -> id)) }
    }
  }

  def update(id: String) = Action(parse.json) { request =>
    Json.fromJson[T](request.body)(res.reader).map{ t =>
      Async{
        res.update(id, t).map{ _ => Ok(Json.obj("id" -> id)) }
      }
    }.recoverTotal{ e => BadRequest(JsError.toFlatJson(e)) }
  }

  def find = Action { request =>
    val q = request.queryString.get("q").flatMap(_.headOption).getOrElse("")
    val v = request.queryString.get("view").flatMap(_.headOption).getOrElse("")
    val doc = request.queryString.get("doc").flatMap(_.headOption).getOrElse("")
    Async {
      bucket.view(doc, v)(bucket, res.ctx).flatMap { view =>
        val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE)
          .setRangeStart(ComplexKey.of(q))
          .setRangeEnd(ComplexKey.of(s"$q\uefff"))
        res.find(view, query)
      }.map( s => Ok(Json.toJson(s)(Writes.list(res.writer))) )
    }
  }

  def findStream = Action { request =>
    val q = request.queryString.get("q").flatMap(_.headOption).getOrElse("")
    val v = request.queryString.get("view").flatMap(_.headOption).getOrElse("")
    val doc = request.queryString.get("doc").flatMap(_.headOption).getOrElse("")
    Async {
      bucket.view(doc, v)(bucket, res.ctx).flatMap { view =>
        val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE)
          .setRangeStart(ComplexKey.of(q))
          .setRangeEnd(ComplexKey.of(s"$q\uefff"))
        res.findStream(view, query)
      }.map { s => Ok.stream(
        s.map( it => Json.toJson(it)(res.writer) ).andThen(Enumerator.eof) )
      }
    }
  }
}