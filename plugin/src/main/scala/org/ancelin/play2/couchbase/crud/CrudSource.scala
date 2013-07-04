package org.ancelin.play2.couchbase.crud

import scala.concurrent.{Future, ExecutionContext}
import play.api.libs.json._
import com.couchbase.client.protocol.views.{ComplexKey, Stale, Query, View}
import play.api.libs.iteratee.{Enumerator}
import play.api.mvc._
import org.ancelin.play2.couchbase.{Couchbase, CouchbaseBucket}
import java.util.UUID
import net.spy.memcached.ops.OperationStatus
import play.core.Router
import scala.Some
import play.api.libs.json.JsObject

// Higly inspired (not to say copied ;)) from https://github.com/mandubian/play-autosource
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

trait CrudController extends Controller {
  def insert: EssentialAction

  def get(id: String): EssentialAction
  def delete(id: String): EssentialAction
  def update(id: String): EssentialAction

  def find: EssentialAction
  def findStream: EssentialAction
}

abstract class CrudRouterController(implicit idBindable: PathBindable[String])
  extends Router.Routes
  with CrudController {

  private var path: String = ""
  private val Slash        = "/?".r
  private val Id           = "/([^/]+)/?".r
  private val Find         = "/find/?".r
  private val Stream       = "/stream/?".r

  def withId(id: String, action: String => EssentialAction) =
    idBindable.bind("id", id).fold(badRequest, action)

  def setPrefix(prefix: String) {
    path = prefix
  }

  def prefix = path
  def documentation = Nil
  def routes = new scala.runtime.AbstractPartialFunction[RequestHeader, Handler] {
    override def applyOrElse[A <: RequestHeader, B >: Handler](rh: A, default: A => B) = {
      if (rh.path.startsWith(path)) {
        (rh.method, rh.path.drop(path.length)) match {
          case ("GET",    Stream())    => findStream
          case ("GET",    Id(id))      => withId(id, get)
          case ("GET",    Slash())     => find
          case ("PUT",    Id(id))      => withId(id, update)
          case ("POST",   Find())      => find
          case ("POST",   Slash())     => insert
          case ("DELETE", Id(id))      => withId(id, delete)
          case _                       => default(rh)
        }
      } else {
        default(rh)
      }
    }

    def isDefinedAt(rh: RequestHeader) =
      if (rh.path.startsWith(path)) {
        (rh.method, rh.path.drop(path.length)) match {
          case ("GET",    Stream()   | Id(_)    | Slash()) => true
          case ("PUT",    Id(_))                           => true
          case ("POST",   Slash())                         => true
          case ("DELETE", Id(_))                           => true
          case _ => false
        }
      } else {
        false
      }
  }
}

abstract class CouchbaseCrudSourceController[T:Format] extends CrudRouterController {

  import org.ancelin.play2.couchbase.CouchbaseImplicitConversion.Couchbase2ClientWrapper
  import play.api.Play.current

  val bucket: CouchbaseBucket

  val defaultDesignDocname = ""
  val defaultViewName= ""

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
        case Some(tid) => Ok(Json.toJson(tid._1)(res.writer))
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
    val v = request.queryString.get("view").flatMap(_.headOption).getOrElse(defaultViewName)
    val doc = request.queryString.get("doc").flatMap(_.headOption).getOrElse(defaultDesignDocname)
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
    val v = request.queryString.get("view").flatMap(_.headOption).getOrElse(defaultViewName)
    val doc = request.queryString.get("doc").flatMap(_.headOption).getOrElse(defaultDesignDocname)
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

