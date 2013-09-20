package org.ancelin.play2.couchbase.plugins

import play.api.{PlayException, Plugin, Application, Play}
import play.api.Play.current
import play.api.libs.ws.WS
import scala.concurrent.{ExecutionContext, Future}
import play.api.libs.iteratee.{Enumeratee, Iteratee, Concurrent, Enumerator}
import play.api.libs.json._
import play.api.libs.json.JsArray
import play.api.libs.ws.WS.WSRequestHolder
import play.api.libs.json.JsObject
import scala.Some

class CouchbaseN1QLPlugin(app: Application) extends Plugin {

  var queryBase: Option[WSRequestHolder] = None

  override def onStart {
    val host = Play.configuration.getString("couchbase.n1ql.host").getOrElse(throw new PlayException("Cannot find N1QL host", "Cannot find N1QL host in couchbase.n1ql conf."))
    val port = Play.configuration.getString("couchbase.n1ql.port").getOrElse(throw new PlayException("Cannot find N1QL port", "Cannot find N1QL port in couchbase.n1ql conf."))
    queryBase = Some(WS.url(s"http://$host:$port/query"))
  }
}

/**
 * trait EnumeratorWrapper[T] {
 *   self: Enumerator[T] =>
 *
 *   def map[U](mapper: T => U)(implicit ec: ExecutionContext): Enumerator[U] =
 *
 *   def mapM[U](mapper: T => Future[U])(implicit ec: ExecutionContext): Enumerator[U] =
 *
 *   def collect[U](pf: PartialFunction[T,U])(implicit ec: ExecutionContext): Enumerator[U] =
 *
 *   def filter(predicate: T => Boolean)(implicit ec: ExecutionContext): Enumerator[T] =
 *
 *   def filterNot(predicate: T => Boolean)(implicit ec: ExecutionContext): Enumerator[T] =
 * }
 **/

class N1QLQuery(query: String, base: WSRequestHolder) {
  def enumerateJson(implicit ec: ExecutionContext): Future[Enumerator[JsObject]] = {
    toJsArray(ec).map { arr =>
      Enumerator.enumerate(arr.value) &> Enumeratee.map[JsValue](_.as[JsObject])
    }
  }

  def enumerate[T](implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    enumerateJson(ec).map { e =>
      e &> Enumeratee.map[JsObject](r.reads(_)) &> Enumeratee.collect[JsResult[T]] { case JsSuccess(value, _) => value }
    }
  }

  def asJsonEnumerator(implicit ec: ExecutionContext): Enumerator[JsObject] = {
    Concurrent.unicast[JsObject](onStart = c => enumerateJson(ec).map(_(Iteratee.foreach[JsObject](c.push).map(_ => c.eofAndEnd()))))
  }

  def asEnumerator[T](implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Concurrent.unicast[T](onStart = c => enumerate[T](r, ec).map(_(Iteratee.foreach[T](c.push).map(_ => c.eofAndEnd()))))
  }

  def toJsArray(implicit ec: ExecutionContext): Future[JsArray] = {
    base.post(Map("q" -> Seq(query))).map { response =>
      (response.json \ "resultset").as[JsArray]
    }
  }

  def toList[T](implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    enumerate[T](r, ec).flatMap(_(Iteratee.getChunks[T]).flatMap(_.run))
  }

  def headOption[T](implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    enumerate[T](r, ec).flatMap(_(Iteratee.head[T]).flatMap(_.run))
  }
}

object CouchbaseN1QLPlugin {

  lazy val N1QLPlugin = Play.current.plugin[CouchbaseN1QLPlugin] match {
    case Some(plugin) => plugin
    case _ => throw new PlayException("CouchbaseN1QLPlugin Error", "Cannot find an instance of CouchbaseN1QLPlugin.")
  }

  def N1QL(query: String): N1QLQuery = {
    new N1QLQuery(query, N1QLPlugin.queryBase.getOrElse(throw new PlayException("Cannot find N1QL connection", "Cannot find N1QL connection.")))
  }
}
