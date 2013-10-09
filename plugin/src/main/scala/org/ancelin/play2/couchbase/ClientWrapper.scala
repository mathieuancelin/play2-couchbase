package org.ancelin.play2.couchbase

import play.api.libs.json._
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import com.couchbase.client.protocol.views._
import collection.JavaConversions._
import net.spy.memcached.{ReplicateTo, PersistTo}
import akka.actor.ActorSystem
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import net.spy.memcached.internal.{BulkFuture, OperationFuture}
import com.couchbase.client.internal.HttpFuture
import play.api.Play.current
import play.api.{PlayException, Play}
import play.api.libs.iteratee.{Concurrent, Enumeratee, Iteratee, Enumerator}
import scala.concurrent.Promise
import net.spy.memcached.transcoders.Transcoder
import org.ancelin.play2.java.couchbase.Row
import play.api.libs.json.JsSuccess
import scala.Some
import play.api.libs.json.JsObject

class JsonValidationException(message: String, errors: JsObject) extends RuntimeException(message + " : " + Json.stringify(errors))
class OperationFailedException(status: OperationStatus) extends RuntimeException(status.getMessage)

case class RawRow(document: String, id: String, key: String, value: String) {
  def toTuple = (document, id, key, value)
}

private case class JsRow[T](document: JsResult[T], id: String, key: String, value: String) {
  def toTuple = (document, id, key, value)
}

case class TypedRow[T](document: T, id: String, key: String, value: String) {
  def toTuple = (document, id, key, value)
}

class QueryEnumerator[T](futureEnumerator: Future[Enumerator[T]]) {
  def enumerate: Future[Enumerator[T]] = futureEnumerator
  def enumerated(implicit ec: ExecutionContext): Enumerator[T] = {
    //val (en, channel) = Concurrent.broadcast[T]
    //futureEnumerator.map(e => e(Iteratee.foreach[T](channel.push).map(_ => channel.eofAndEnd())))
    //en
    Concurrent.unicast[T](onStart = c => futureEnumerator.map(_(Iteratee.foreach[T](c.push).map(_ => c.eofAndEnd()))))
  }
  def toList(implicit ec: ExecutionContext): Future[List[T]] =
    futureEnumerator.flatMap(_(Iteratee.getChunks[T]).flatMap(_.run))

  def headOption(implicit ec: ExecutionContext): Future[Option[T]] =
    futureEnumerator.flatMap(_(Iteratee.head[T]).flatMap(_.run))

  /*def map[U](mapper: T => U)(implicit ec: ExecutionContext): QueryEnumerator[U] =
    QueryEnumerator[U](futureEnumerator.map(_.map(mapper)))

  def mapM[U](mapper: T => Future[U])(implicit ec: ExecutionContext): QueryEnumerator[U] =
    QueryEnumerator[U](futureEnumerator.map(_ &> Enumeratee.mapM[T](mapper)))

  def collect[U](pf: PartialFunction[T,U])(implicit ec: ExecutionContext): QueryEnumerator[U] =
    QueryEnumerator[U](futureEnumerator.map(_ &> Enumeratee.collect[T](pf)))

  def filter(predicate: T => Boolean)(implicit ec: ExecutionContext): QueryEnumerator[T] =
    QueryEnumerator[T](futureEnumerator.map(_ &> Enumeratee.filter[T](predicate)))

  def filterNot(predicate: T => Boolean)(implicit ec: ExecutionContext): QueryEnumerator[T] =
    QueryEnumerator[T](futureEnumerator.map(_ &> Enumeratee.filterNot[T](predicate)))

  def take(n: Int)(implicit ec: ExecutionContext): QueryEnumerator[T] =
    QueryEnumerator[T](futureEnumerator.map(_ &> Enumeratee.take[T](n)))

  def takeWhile(predicate: T => Boolean)(implicit ec: ExecutionContext): QueryEnumerator[T] =
    QueryEnumerator[T](futureEnumerator.map(_ &> Enumeratee.takeWhile[T](predicate)))

  def drop(n: Int)(implicit ec: ExecutionContext): QueryEnumerator[T] =
    QueryEnumerator[T](futureEnumerator.map(_ &> Enumeratee.drop[T](n)))

  def dropWhile(predicate: T => Boolean)(implicit ec: ExecutionContext): QueryEnumerator[T] =
    QueryEnumerator[T](futureEnumerator.map(_ &> Enumeratee.dropWhile[T](predicate))) */
}

object QueryEnumerator {
  def apply[T](enumerate: Future[Enumerator[T]]): QueryEnumerator[T] = new QueryEnumerator[T](enumerate)
}

// Yeah I know JavaFuture.get is really ugly, but what can I do ???
// http://stackoverflow.com/questions/11529145/how-do-i-wrap-a-java-util-concurrent-future-in-an-akka-future
trait ClientWrapper {

  def find[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext) = searchValues(docName, viewName)(query)(bucket, r, ec).toList(ec)
  def find[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext) = searchValues(view)(query)(bucket, r, ec).toList(ec)

  ///////////////////////////////////////////////////////////////////////////////////////////////////

  def rawSearch(docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[RawRow] = {
    QueryEnumerator(view(docName, viewName).flatMap {
      case view: View => rawSearch(view)(query)(bucket, ec).enumerate
      case _ => Future.failed(new PlayException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  def rawSearch(view: View)(query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[RawRow] = {
    QueryEnumerator(wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncQuery(view, query), ec ).map { results =>
      Enumerator.enumerate(results.iterator()) &> Enumeratee.map[ViewRow](r => RawRow(r.getDocument.asInstanceOf[String], r.getId, r.getKey, r.getValue))
    })
  }

  def search[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = {
    QueryEnumerator(view(docName, viewName).flatMap {
      case view: View => search(view)(query)(bucket, r, ec).enumerate
      case _ => Future.failed(new PlayException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  def search[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = {
    QueryEnumerator(rawSearch(view)(query)(bucket, ec).enumerate.map { enumerator =>
      enumerator &> Enumeratee.map[RawRow](row => JsRow[T](r.reads(Json.parse(row.document)), row.id, row.key, row.value)) &>
        Enumeratee.collect[JsRow[T]] {
          case JsRow(JsSuccess(doc, _), id, key, value) => TypedRow[T](doc, id, key, value)
          case JsRow(JsError(errors), _, _, _) if Constants.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(errors))
        }
    })
  }

  def searchValues[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(view(docName, viewName).flatMap {
      case view: View => searchValues(view)(query)(bucket, r, ec).enumerate
      case _ => Future.failed(new PlayException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    })
  }

  def searchValues[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(search[T](view)(query)(bucket, r, ec).enumerate.map { enumerator =>
      enumerator &> Enumeratee.map[TypedRow[T]](_.document)
    })
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    pollQuery[T](doc, view, query, everyMillis, { chunk: T => true })(bucket, r, ec)
  }

  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long, filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.repeatM(
      play.api.libs.concurrent.Promise.timeout(Some, everyMillis, TimeUnit.MILLISECONDS).flatMap(_ => find[T](doc, view)(query)(bucket, r, ec))
    ).through( Enumeratee.mapConcat[List[T]](identity) ).through( Enumeratee.filter[T]( filter ) )
  }

  def repeatQuery[T](doc: String, view: String, query: Query, trigger: Future[AnyRef])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, trigger, { chunk: T => true })(bucket, r, ec)
  }

  def repeatQuery[T](doc: String, view: String, query: Query, trigger: Future[AnyRef], filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.repeatM(
      trigger.flatMap { _ => find[T](doc, view)(query)(bucket, r, ec) }
    ).through( Enumeratee.mapConcat[List[T]](identity) ).through( Enumeratee.filter[T]( filter ) )
  }

  def repeatQuery[T](doc: String, view: String, query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, { chunk: T => true })(bucket, r, ec)
  }

  def repeatQuery[T](doc: String, view: String, query: Query, filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](doc, view, query, Future.successful(Some),filter)(bucket, r, ec)
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////

  def view(docName: String, viewName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[View] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGetView(docName, viewName), ec )
  }

  def spatialView(docName: String, viewName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[SpatialView] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGetSpatialView(docName, viewName), ec )
  }

  def designDocument(docName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[DesignDocument] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGetDesignDocument(docName), ec )
  }

  def createDesignDoc(name: String, value: JsObject)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncCreateDesignDoc(name, Json.stringify(value)), ec )
  }

  def createDesignDoc(name: String, value: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncCreateDesignDoc(name, value), ec )
  }

  def createDesignDoc(value: DesignDocument)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncCreateDesignDoc(value), ec )
  }

  def deleteDesignDoc(name: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncDeleteDesignDoc(name), ec )
  }

  def keyStats(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Map[String, String]] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.getKeyStats(key), ec ).map(_.toMap)
  }

  def get[T](key: String, tc: Transcoder[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[T]] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGet(key, tc), ec ) map {
      case value: T => Some[T](value)
      case _ => None
    }
  }

  def rawFetch(keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[(String, String)] = {
    QueryEnumerator(keysEnumerator.apply(Iteratee.getChunks[String]).flatMap(_.run).flatMap { keys =>
      wrapJavaFutureInFuture( bucket.couchbaseClient.asyncGetBulk(keys), ec ).map { results =>
        Enumerator.enumerate(results.toList)
      }.map { enumerator =>
        enumerator &> Enumeratee.collect[(String, AnyRef)] { case (k: String, v: String) => (k, v) }
      }
    })
  }

  def fetch[T](keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = {
    QueryEnumerator(rawFetch(keysEnumerator)(bucket, ec).enumerate.map { enumerator =>
      enumerator &> Enumeratee.map[(String, String)]( t => (t._1, r.reads(Json.parse(t._2))) ) &> Enumeratee.collect[(String, JsResult[T])] {
        case (k: String, JsSuccess(value, _)) => (k, value)
        case (k: String, JsError(errors)) if Constants.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(errors))
      }
    })
  }

  def fetchValues[T](keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(fetch[T](keysEnumerator)(bucket, r, ec).enumerate.map { enumerator =>
      enumerator &> Enumeratee.map[(String, T)](_._2)
    })
  }

  def fetch[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = {
    fetch[T](Enumerator.enumerate(keys))(bucket, r, ec)
  }

  def fetchValues[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    fetchValues[T](Enumerator.enumerate(keys))(bucket, r, ec)
  }

  // TODO : check for perfs issues here
  def get[T](key: String)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    fetchValues[T](Enumerator(key))(bucket, r, ec).headOption(ec)
  }

  def getWithKey[T](key: String)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Option[(String, T)]] = {
    fetch[T](Enumerator(key))(bucket, r, ec).headOption(ec)
  }

  def fetchWithKeys[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    fetch[T](keys)(bucket, r, ec).toList(ec).map(_.toMap)
  }

  def fetchWithKeys[T](keys: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    fetch[T](keys)(bucket, r, ec).toList(ec).map(_.toMap)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Counter Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def incr(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncIncr(key, by), ec )
  }

  def incr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncIncr(key, by), ec )
  }

  def decr(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncDecr(key, by), ec )
  }

  def decr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncDecr(key, by), ec )
  }

  def incrAndGet(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Int] = {
    Future[Long]( bucket.couchbaseClient.incr(key, by) )(ec).map(_.toInt)
  }

  def incrAndGet(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Long] = {
    Future[Long]( bucket.couchbaseClient.incr(key, by) )(ec)
  }

  def decrAndGet(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Int] = {
    Future[Long]( bucket.couchbaseClient.decr(key, by) )(ec).map(_.toInt)
  }

  def decrAndGet(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Long] = {
    Future[Long]( bucket.couchbaseClient.decr(key, by) )(ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Set Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def set[T <: {def id: String}](value: T, exp: Int)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](value.id, exp, value)(bucket, w, ec)
  }

  def set[T <: {def id: String}](value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](value.id, value)(bucket, w, ec)
  }

  def setWithKey[T](value: T, key: T => String, exp: Int)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](key(value), exp, value)(bucket, w, ec)
  }

  def setWithKey[T](value: T, key: T => String)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](key(value), value)(bucket, w, ec)
  }

  def set[T <: {def id:String}](value: T, exp: Int, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](value.id, exp, value, replicateTo)(bucket, w, ec)
  }

  def set[T <: {def id:String}](value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](value.id, value, replicateTo)(bucket, w, ec)
  }

  def setWithKey[T](value: T, key: T => String, exp: Int, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](key(value), exp, value, replicateTo)(bucket, w, ec)
  }

  def setWithKey[T](value: T, key: T => String, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](key(value), value, replicateTo)(bucket, w, ec)
  }

  def set[T <: {def id:String}](value: T, exp: Int, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](value.id, exp, value, persistTo)(bucket, w, ec)
  }

  def set[T <: {def id:String}](value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](value.id, value, persistTo)(bucket, w, ec)
  }

  def setWithKey[T](value: T, key: T => String, exp: Int, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](key(value), exp, value, persistTo)(bucket, w, ec)
  }

  def setWithKey[T](value: T, key: T => String, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](key(value), value, persistTo)(bucket, w, ec)
  }

  def set[T <: {def id:String}](value: T, exp: Int, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](value.id, exp, value, persistTo, replicateTo)(bucket, w, ec)
  }

  def set[T <: {def id:String}](value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](value.id, value, persistTo, replicateTo)(bucket, w, ec)
  }

  def setWithKey[T](value: T, key: T => String, exp: Int, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](key(value), exp, value, persistTo, replicateTo)(bucket, w, ec)
  }

  def setWithKey[T](value: T, key: T => String, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](key(value), value, persistTo, replicateTo)(bucket, w, ec)
  }

  def set[T](key: String, value: T, tc: Transcoder[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, Constants.expiration, value, tc), ec )
  }

  def set[T](key: String, exp: Int, value: T, tc: Transcoder[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, exp, value, tc), ec )
  }

  def set[T](key: String, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, Constants.expiration, value)(bucket, w, ec)
  }

  def set[T](key: String, exp: Int, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value))), ec )
  }

  def set[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value)), replicateTo), ec )
  }
  
  def set[T](key: String, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, Constants.expiration, value, replicateTo)(bucket, w, ec)
  }
  
  def set[T](key: String, exp: Int, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value)), persistTo), ec )
  }
  
  def set[T](key: String, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, Constants.expiration, value, persistTo)(bucket, w, ec)
  }
  
  def set[T](key: String, exp: Int, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), ec )
  }
  
  def set[T](key: String, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, Constants.expiration, value, persistTo, replicateTo)(bucket, w, ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Add Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def add[T <: {def id:String}](value: T, exp: Int)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](value.id, exp, value)(bucket, w, ec)
  }

  def add[T <: {def id:String}](value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](value.id, value)(bucket, w, ec)
  }

  def addWithKey[T](value: T, key: T => String, exp: Int)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](key(value), exp, value)(bucket, w, ec)
  }

  def addWithKey[T](value: T, key: T => String)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](key(value), value)(bucket, w, ec)
  }

  def add[T <: {def id:String}](value: T, exp: Int, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](value.id, exp, value, replicateTo)(bucket, w, ec)
  }

  def add[T <: {def id:String}](value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](value.id, value, replicateTo)(bucket, w, ec)
  }

  def addWithKey[T](value: T, key: T => String, exp: Int, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](key(value), exp, value, replicateTo)(bucket, w, ec)
  }

  def addWithKey[T](value: T, key: T => String, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](key(value), value, replicateTo)(bucket, w, ec)
  }

  def add[T <: {def id:String}](value: T, exp: Int, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](value.id, exp, value, persistTo)(bucket, w, ec)
  }

  def add[T <: {def id:String}](value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](value.id, value, persistTo)(bucket, w, ec)
  }

  def addWithKey[T](value: T, key: T => String, exp: Int, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](key(value), exp, value, persistTo)(bucket, w, ec)
  }

  def addWithKey[T](value: T, key: T => String, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](key(value), value, persistTo)(bucket, w, ec)
  }

  def add[T <: {def id:String}](value: T, exp: Int, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](value.id, exp, value, persistTo, replicateTo)(bucket, w, ec)
  }

  def add[T <: {def id:String}](value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](value.id, value, persistTo, replicateTo)(bucket, w, ec)
  }

  def addWithKey[T](value: T, key: T => String, exp: Int, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](key(value), exp, value, persistTo, replicateTo)(bucket, w, ec)
  }

  def addWithKey[T](value: T, key: T => String, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](key(value), value, persistTo, replicateTo)(bucket, w, ec)
  }

  def add[T](key: String, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, Constants.expiration, value)(bucket, w, ec)
  }

  def add[T](key: String, exp: Int, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.add(key, exp, Json.stringify(w.writes(value))), ec )
  }

  def add[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.add(key, exp, Json.stringify(w.writes(value)), replicateTo), ec )
  }
  
  def add[T](key: String, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, Constants.expiration, value, replicateTo)(bucket, w, ec)
  }
  
  def add[T](key: String, exp: Int, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.add(key, exp, Json.stringify(w.writes(value)), persistTo), ec )
  }
  
  def add[T](key: String, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, Constants.expiration, value, persistTo)(bucket, w, ec)
  }
  
  def add[T](key: String, exp: Int, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.add(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), ec )
  }
  
  def add[T](key: String, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, Constants.expiration, value, persistTo, replicateTo)(bucket, w, ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Replace Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def replace[T <: {def id:String}](value: T, exp: Int)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](value.id, exp, value)(bucket, w, ec)
  }

  def replace[T <: {def id:String}](value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](value.id, value)(bucket, w, ec)
  }

  def replaceWithKey[T](value: T, key: T => String, exp: Int)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](key(value), exp, value)(bucket, w, ec)
  }

  def replaceWithKey[T](value: T, key: T => String)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](key(value), value)(bucket, w, ec)
  }

  def replace[T <: {def id:String}](value: T, exp: Int, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](value.id, exp, value, replicateTo)(bucket, w, ec)
  }

  def replace[T <: {def id:String}](value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](value.id, value, replicateTo)(bucket, w, ec)
  }

  def replaceWithKey[T](value: T, key: T => String, exp: Int, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](key(value), exp, value, replicateTo)(bucket, w, ec)
  }

  def replaceWithKey[T](value: T, key: T => String, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](key(value), value, replicateTo)(bucket, w, ec)
  }

  def replace[T <: {def id:String}](value: T, exp: Int, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](value.id, exp, value, persistTo)(bucket, w, ec)
  }

  def replace[T <: {def id:String}](value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](value.id, value, persistTo)(bucket, w, ec)
  }

  def replaceWithKey[T](value: T, key: T => String, exp: Int, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](key(value), exp, value, persistTo)(bucket, w, ec)
  }

  def replaceWithKey[T](value: T, key: T => String, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](key(value), value, persistTo)(bucket, w, ec)
  }

  def replace[T <: {def id:String}](value: T, exp: Int, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](value.id, exp, value, persistTo, replicateTo)(bucket, w, ec)
  }

  def replace[T <: {def id:String}](value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](value.id, value, persistTo, replicateTo)(bucket, w, ec)
  }

  def replaceWithKey[T](value: T, key: T => String, exp: Int, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](key(value), exp, value, persistTo, replicateTo)(bucket, w, ec)
  }

  def replaceWithKey[T](value: T, key: T => String, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](key(value), value, persistTo, replicateTo)(bucket, w, ec)
  }

  def replace[T](key: String, exp: Int, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.replace(key, exp, Json.stringify(w.writes(value))), ec )
  }

  def replace[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.replace(key, exp, Json.stringify(w.writes(value)), replicateTo), ec )
  }
  
  def replace[T](key: String, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
     replace(key, Constants.expiration, value, replicateTo)(bucket, w, ec)
  }
  
  def replace[T](key: String, exp: Int, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.replace(key, exp, Json.stringify(w.writes(value)), persistTo), ec )
  }
  
  def replace[T](key: String, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, Constants.expiration, value, persistTo)(bucket, w, ec)
  }
  
  def replace[T](key: String, exp: Int, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.replace(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), ec )
  }
  
  def replace[T](key: String, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, Constants.expiration, value, persistTo,replicateTo)(bucket, w, ec)
  }

  def replace[T](key: String, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, Constants.expiration, value)(bucket, w, ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Delete Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def delete(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.delete(key), ec )
  }

  def delete(key: String, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.delete(key, replicateTo), ec )
  }

  def delete(key: String, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.delete(key, persistTo), ec )
  }

  def delete(key: String, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.delete(key, persistTo, replicateTo), ec )
  }

  def delete[T <: {def id:String}](value: T)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.delete(value.id), ec )
  }

  def delete[T <: {def id:String}](value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.delete(value.id, replicateTo), ec )
  }

  def delete[T <: {def id:String}](value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.delete(value.id, persistTo), ec )
  }

  def delete[T <: {def id:String}](value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.delete(value.id, persistTo, replicateTo), ec )
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Flush Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def flush(delay: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.flush(delay), ec )
  }

  def flush()(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
     flush(Constants.expiration)(bucket, ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Java Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def javaReplace(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.replace(key, exp, value, persistTo, replicateTo), ec )
  }

  def javaAdd(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.add(key, exp, value, persistTo, replicateTo), ec )
  }

  def javaSet(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, exp, value, persistTo, replicateTo), ec )
  }

  def javaGet(key: String, bucket: CouchbaseBucket, ec: ExecutionContext): Future[String] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGet(key), ec ).map {
      case s: String => s
    }(ec)
  }

  def javaGet[T](key: String, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGet(key), ec ).flatMap { f =>
      f match {
        case value: String => Future.successful(play.libs.Json.fromJson(play.libs.Json.parse(value), clazz))
        case _ => Future.failed(new NullPointerException)
      }
    }(ec)
  }

  def javaOptGet[T](key: String, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[play.libs.F.Option[T]] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGet(key), ec ).map { f =>
      val opt: play.libs.F.Option[T] = f match {
        case value: String => play.libs.F.Option.Some[T](play.libs.Json.fromJson(play.libs.Json.parse(value), clazz))
        case _ => play.libs.F.Option.None[T]()
      }
      opt
    }(ec)
  }

  def javaFind[T](docName:String, viewName: String, query: Query, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.Collection[T]] = {
    view(docName, viewName)(bucket, ec).flatMap { view =>
      javaFind[T](view, query, clazz, bucket, ec)
    }(ec)
  }

  def javaFind[T](view: View, query: Query, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.Collection[T]] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncQuery(view, query), ec ).map { results =>
      asJavaCollection(results.iterator().map { result =>
        play.libs.Json.fromJson(play.libs.Json.parse(result.getDocument.asInstanceOf[String]), clazz)
      }.toList)
    }(ec)
  }

  def javaFullFind[T](docName:String, viewName: String, query: Query, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.Collection[Row[T]]] = {
    view(docName, viewName)(bucket, ec).flatMap { view =>
      javaFullFind[T](view, query, clazz, bucket, ec)
    }(ec)
  }

  def javaFullFind[T](view: View, query: Query, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.Collection[Row[T]]] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncQuery(view, query), ec ).map { results =>
      asJavaCollection(results.iterator().map { result =>
        new Row[T](play.libs.Json.fromJson(play.libs.Json.parse(result.getDocument.asInstanceOf[String]), clazz), result.getId, result.getKey, result.getValue )
      }.toList)
    }(ec)
  }

  def javaView(docName: String, viewName: String, bucket: CouchbaseBucket, ec: ExecutionContext): Future[View] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGetView(docName, viewName), ec )
  }

  def asJavaLong(f: Future[Long], ec: ExecutionContext): Future[java.lang.Long] = {
    f.map { (v: Long) => v: java.lang.Long }(ec)
  }

  def asJavaInt(f: Future[Int], ec: ExecutionContext): Future[java.lang.Integer] = {
    f.map { (v: Int) => v: java.lang.Integer }(ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Private API to manage Java Futures
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  private def wrapJavaFutureInPureFuture[T](javaFuture: java.util.concurrent.Future[T], ec: ExecutionContext): Future[T] = {
    if (Polling.pollingFutures) {
      val promise = Promise[T]()
      pollJavaFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      promise.future
    } else {
       Future {
         javaFuture.get(Constants.timeout, TimeUnit.MILLISECONDS)
       }(ec)
    }
  }

  private def wrapJavaFutureInFuture[T](javaFuture: OperationFuture[T], ec: ExecutionContext): Future[OperationStatus] = {
    if (Polling.pollingFutures) {
      val promise = Promise[OperationStatus]()
      pollCouchbaseFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      promise.future
    } else {
      Future {
        javaFuture.get(Constants.timeout, TimeUnit.MILLISECONDS)
        javaFuture.getStatus
      }(ec).flatMap { status =>
        if (!Constants.failWithOpStatus) {
          Future(status)(ec)
        } else {
          if (status.isSuccess) {
            Future.successful(status)
          } else {
            Future.failed(new OperationFailedException(status))
          }
        }
      }(ec)
    }
  }

  private def wrapJavaFutureInFuture[T](javaFuture: BulkFuture[T], ec: ExecutionContext): Future[T] = {
    if (Polling.pollingFutures) {
      val promise = Promise[T]()
      pollJavaFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      promise.future
    } else {
      Future {
        javaFuture.get(Constants.timeout, TimeUnit.MILLISECONDS)
      }(ec)
    }
  }

  private def wrapJavaFutureInFuture[T](javaFuture: HttpFuture[T], ec: ExecutionContext): Future[OperationStatus] = {
    if (Polling.pollingFutures) {
      val promise = Promise[OperationStatus]()
      pollCouchbaseFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      promise.future
    } else {
      Future {
        javaFuture.get(Constants.timeout, TimeUnit.MILLISECONDS)
        javaFuture.getStatus
      }(ec).flatMap { status =>
        if (!Constants.failWithOpStatus) {
          Future(status)(ec)
        } else {
          if (status.isSuccess) {
            Future.successful(status)
          } else {
            Future.failed(new OperationFailedException(status))
          }
        }
      }(ec)
    }
  }

  private def pollJavaFutureUntilDoneOrCancelled[T](javaFuture: java.util.concurrent.Future[T], promise: Promise[T], ec: ExecutionContext) {
    if (javaFuture.isDone || javaFuture.isCancelled) {
      promise.success(javaFuture.get(Constants.timeout, TimeUnit.MILLISECONDS))
    } else {
      Polling.system.scheduler.scheduleOnce(FiniteDuration(Polling.delay, TimeUnit.MILLISECONDS)) {
        pollJavaFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      }(ec)
    }
  }

  private def pollCouchbaseFutureUntilDoneOrCancelled[T](javaFuture: java.util.concurrent.Future[T], promise: Promise[OperationStatus], ec: ExecutionContext) {
    if (javaFuture.isDone || javaFuture.isCancelled) {
      javaFuture match {
        case o: OperationFuture[T] => {
          o.get(Constants.timeout, TimeUnit.MILLISECONDS)
          if (!Constants.failWithOpStatus) {
            promise.success(o.getStatus)
          } else {
            if (o.getStatus.isSuccess) promise.success(o.getStatus)
            else promise.failure(new OperationFailedException(o.getStatus))
          }
        }
        case h: HttpFuture[T] => {
          h.get(Constants.timeout, TimeUnit.MILLISECONDS)
          if (!Constants.failWithOpStatus) {
            promise.success(h.getStatus)
          } else {
            if (h.getStatus.isSuccess) promise.success(h.getStatus)
            else promise.failure(new OperationFailedException(h.getStatus))
          }
        }
      }
    } else {
      Polling.system.scheduler.scheduleOnce(FiniteDuration(Polling.delay, TimeUnit.MILLISECONDS)) {
        pollCouchbaseFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      }(ec)
    }
  }
}

object Constants {
  val expiration: Int = -1
  val jsonStrictValidation = Play.configuration.getBoolean("couchbase.json.validate").getOrElse(true)
  val usePlayEC = Play.configuration.getBoolean("couchbase.useplayec").getOrElse(false)
  val failWithOpStatus = Play.configuration.getBoolean("couchbase.failfutures").getOrElse(false)
  val timeout: Long = Play.configuration.getLong("couchbase.execution-context.timeout").getOrElse(1000L)
  implicit val defaultPersistTo: PersistTo = PersistTo.ZERO
  implicit val defaultReplicateTo: ReplicateTo = ReplicateTo.ZERO
  if (jsonStrictValidation) {
    play.api.Logger("CouchbasePlugin").info("Failing on bad JSON structure enabled.")
  }
  if (failWithOpStatus) {
    play.api.Logger("CouchbasePlugin").info("Failing Futures on failed OperationStatus enabled.")
  }
}

object Polling {
  val delay: Long = Play.configuration.getLong("couchbase.execution-context.polldelay").getOrElse(50L)
  val pollingFutures: Boolean = Play.configuration.getBoolean("couchbase.execution-context.pollfutures").getOrElse(false)
  val system = ActorSystem("JavaFutureToScalaFuture")
  if (pollingFutures) {
    play.api.Logger("CouchbasePlugin").info("Using polling to wait for Java Future.")
  }
}
