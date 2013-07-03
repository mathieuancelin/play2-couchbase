package org.ancelin.play2.couchbase

import play.api.libs.json._
import scala.concurrent.{Promise, Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import com.couchbase.client.protocol.views.{SpatialView, DesignDocument, Query, View}
import collection.JavaConversions._
import net.spy.memcached.{ReplicateTo, PersistTo}
import akka.actor.ActorSystem
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import net.spy.memcached.internal.{BulkFuture, OperationFuture}
import com.couchbase.client.internal.HttpFuture
import play.api.Play.current
import play.api.{PlayException, Play}
import play.api.libs.iteratee.{Enumeratee, Iteratee, Enumerator}
import scala.concurrent.Promise
import play.api.libs.json.JsSuccess
import scala.Some
import play.api.libs.json.JsObject

class JsonValidationException(message: String, errors: JsObject) extends RuntimeException(message + " : " + Json.stringify(errors))
class OperationFailedException(status: OperationStatus) extends RuntimeException(status.getMessage)

// Yeah I know JavaFuture.get is really ugly, but what can I do ???
// http://stackoverflow.com/questions/11529145/how-do-i-wrap-a-java-util-concurrent-future-in-an-akka-future
trait ClientWrapper {

  def findP[T](docName:String, viewName: String)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): PartialFunction[Query, Future[List[T]]] = {
    PartialFunction[Query, Future[List[T]]]((query: Query) => {
      find[T](docName, viewName)(query)(bucket, r, ec)
    })
  }

  def findP[T](view: View)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): PartialFunction[Query, Future[List[T]]] = {
    PartialFunction[Query, Future[List[T]]]((query: Query) => {
      find[T](view)(query)(bucket, r, ec)
    })
  }

  def findPAsEnumerator[T](docName:String, viewName: String)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): PartialFunction[Query, Future[Enumerator[T]]] = {
    PartialFunction[Query, Future[Enumerator[T]]]((query: Query) => {
      findAsEnumerator[T](docName, viewName)(query)(bucket, r, ec)
    })
  }

  def findPAsEnumerator[T](view: View)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): PartialFunction[Query, Future[Enumerator[T]]] = {
    PartialFunction[Query, Future[Enumerator[T]]]((query: Query) => {
      findAsEnumerator[T](view)(query)(bucket, r, ec)
    })
  }

  def find[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    view(docName, viewName)(bucket, ec).flatMap {
      case view: View => find[T](view)(query)(bucket, r, ec)
      case _ => Future.failed(new PlayException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    }
  }

  def find[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncQuery(view, query), ec ).map { results =>
      results.iterator().map { result =>
        result.getDocument match {
          case s: String => r.reads(Json.parse(s)) match {
            case e:JsError => if (Constants.jsonStrictValidation) throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(e.errors)) else None
            case s:JsSuccess[T] => s.asOpt
          }
          case t: T => Some(t)
          case _ => None
        }
      }.toList.filter(_.isDefined).map(_.get)
    }
  }

  def findAsEnumerator[T](view: View)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
     find[T](view)(query)(bucket, r, ec).map(Enumerator.enumerate(_))
  }

  def findAsEnumerator[T](docName:String, viewName: String)(query: Query)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    view(docName, viewName)(bucket, ec).flatMap {
      case view: View => findAsEnumerator[T](view)(query)(bucket, r, ec)
      case _ => Future.failed(new PlayException("Couchbase view error", s"Can't find view $viewName from $docName. Please create it."))
    }
  }

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

  def view(docName: String, viewName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[View] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGetView(docName, viewName), ec )
  }

  def spatialView(docName: String, viewName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[SpatialView] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGetSpatialView(docName, viewName), ec )
  }

  def designDocument(docName: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[DesignDocument[_]] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGetDesignDocument(docName), ec )
  }

  def createDesignDoc(name: String, value: JsObject)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncCreateDesignDoc(name, Json.stringify(value)), ec )
  }

  def createDesignDoc(name: String, value: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncCreateDesignDoc(name, value), ec )
  }

  def createDesignDoc(value: DesignDocument[_])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncCreateDesignDoc(value), ec )
  }

  def deleteDesignDoc(name: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncDeleteDesignDoc(name), ec )
  }

  def keyStats(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Map[String, String]] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.getKeyStats(key), ec ).map(_.toMap)
  }

  def get[T](key: String)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGet(key), ec ).map { f =>
       f match {
         case value: String => r.reads(Json.parse(value)) match {
           case e:JsError => if (Constants.jsonStrictValidation) throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(e.errors)) else None
           case s:JsSuccess[T] => s.asOpt
         }
         case _ => None
       }
    }
  }

  def getBulkWithKeys[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.asyncGetBulk(keys), ec ).map { value =>
      value.filter { tuple =>
        tuple._2 match {
          case s: String => true
          case t: T => true
          case _ => false
        }
      }.map { tuple =>
        tuple._2 match {
          case s: String => r.reads(Json.parse(s)) match {
            case e:JsError => if (Constants.jsonStrictValidation) throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(e.errors)) else (tuple._1, None)
            case s:JsSuccess[T] => (tuple._1, s.asOpt)
          }
          case t: T => (tuple._1, Some(t))
          case _ => (tuple._1, None)
        }
      }.filter(_._2.isDefined).map( v => v._1 -> v._2.get).toMap[String, T]
    }
  }

  def getBulk[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    getBulkWithKeys[T](keys)(bucket, r, ec).map(_.map(_._2).toList)
  }

  def getBulkWithKeys[T](keys: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    keys.apply(Iteratee.getChunks[String]).flatMap { iteratee =>
      iteratee.run
    }.flatMap { values =>
      getBulkWithKeys[T](values)(bucket, r, ec)
    }
  }

  def getBulk[T](keys: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    keys.apply(Iteratee.getChunks[String]).flatMap { iteratee =>
      iteratee.run
    }.flatMap { values =>
      getBulk[T](values)(bucket, r, ec)
    }
  }

  def getBulkWithKeysAsEnumerator[T](keys: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Enumerator[(String, T)]] = {
    getBulkWithKeys[T](keys)(bucket, r, ec).map(Enumerator.enumerate(_))
  }

  def getBulkAsEnumerator[T](keys: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    getBulk[T](keys)(bucket, r, ec).map(Enumerator.enumerate(_))
  }

  def getBulkWithKeysAsEnumerator[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Enumerator[(String, T)]] = {
    getBulkWithKeys[T](keys)(bucket, r, ec).map(Enumerator.enumerate(_))
  }

  def getBulkAsEnumerator[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    getBulk[T](keys)(bucket, r, ec).map(Enumerator.enumerate(_))
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

  def set[T](key: String, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, Constants.expiration, value)(bucket, w, ec)
  }

  def set[T](key: String, exp: Int, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value))), ec )
  }

  def set[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, exp, value, replicateTo), ec )
  }
  
  def set[T](key: String, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, Constants.expiration, value, replicateTo)(bucket, w, ec)
  }
  
  def set[T](key: String, exp: Int, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, exp, value, persistTo), ec )
  }
  
  def set[T](key: String, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, Constants.expiration, value, persistTo)(bucket, w, ec)
  }
  
  def set[T](key: String, exp: Int, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.set(key, exp, value, persistTo, replicateTo), ec )
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
    wrapJavaFutureInFuture( bucket.couchbaseClient.add(key, exp, value, replicateTo), ec )
  }
  
  def add[T](key: String, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, Constants.expiration, value, replicateTo)(bucket, w, ec)
  }
  
  def add[T](key: String, exp: Int, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.add(key, exp, value, persistTo), ec )
  }
  
  def add[T](key: String, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, Constants.expiration, value, persistTo)(bucket, w, ec)
  }
  
  def add[T](key: String, exp: Int, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.add(key, exp, value, persistTo, replicateTo), ec )
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
    wrapJavaFutureInFuture( bucket.couchbaseClient.replace(key, exp, value, replicateTo), ec )
  }
  
  def replace[T](key: String, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
     replace(key, Constants.expiration, value, replicateTo)(bucket, w, ec)
  }
  
  def replace[T](key: String, exp: Int, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.replace(key, exp, value, persistTo), ec )
  }
  
  def replace[T](key: String, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, Constants.expiration, value, persistTo)(bucket, w, ec)
  }
  
  def replace[T](key: String, exp: Int, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( bucket.couchbaseClient.replace(key, exp, value, persistTo, replicateTo), ec )
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

  def javaGet[T](key: String, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    wrapJavaFutureInPureFuture( bucket.couchbaseClient.asyncGet(key), ec ).map { f =>
      f match {
        case value: String => play.libs.Json.fromJson(play.libs.Json.parse(value), clazz)
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
