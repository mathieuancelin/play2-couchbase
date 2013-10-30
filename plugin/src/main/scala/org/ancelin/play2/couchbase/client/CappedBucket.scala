package org.ancelin.play2.couchbase.client

import org.ancelin.play2.couchbase.{CouchbaseRWImplicits, CouchbaseBucket, Couchbase}
import play.api.Application
import com.couchbase.client.protocol.views.{Stale, View, Query}
import play.api.libs.json._
import scala.concurrent.{Future, ExecutionContext}
import java.util.concurrent.TimeUnit
import play.api.libs.iteratee.{Enumeratee, Enumerator}
import net.spy.memcached.{ReplicateTo, PersistTo}
import net.spy.memcached.ops.OperationStatus
import scala.concurrent.duration.Duration

class CappedBucket(name: String, max: Int, reaper: Boolean = true)(implicit app: Application) {

  def bucket = Couchbase.bucket(name)

  private val trigger = setupViews(Couchbase.couchbaseExecutor(app))
  if (reaper) enabledReaper(Couchbase.couchbaseExecutor(app))

  private def docName = "play2couchbase-cappedbucket-designdoc"
  private def viewName = "byNaturalOrder"
  private def cappedRef = "__playcbcapped"
  private def cappedNaturalId = "__playcbcappednatural"
  private def designDoc =
    s"""
      {
        "views":{
           "byNaturalOrder": {
               "map": "function (doc, meta) { if (doc.$cappedRef) { if (doc.$cappedNaturalId) { emit(doc.$cappedNaturalId, null); } } } "
           }
        }
      }
    """

  private def setupViews(ec: ExecutionContext) = {
    bucket.createDesignDoc(docName, designDoc)(ec)
  }

  private def enabledReaper(ec: ExecutionContext) = {
    play.api.libs.concurrent.Akka.system(app).scheduler.schedule(Duration(0, TimeUnit.MILLISECONDS), Duration(1000, TimeUnit.MILLISECONDS))({
      val query = new Query().setIncludeDocs(false).setStale(Stale.FALSE).setDescending(true).setSkip(max)
      bucket.rawSearch(docName, viewName)(query)(ec).enumerated(ec).map { elem =>
        bucket.delete(elem.id.get)(ec)
      }(ec)
    })(ec)
  }

  def oldestOption[T](key: String)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(false).setLimit(1)
    trigger.flatMap(_ => Couchbase.find[T](docName, viewName)(query)(bucket, r, ec).map(_.headOption))
  }

  def lastInsertedOption[T](key: String)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(true).setLimit(1)
    trigger.flatMap(_ => Couchbase.find[T](docName, viewName)(query)(bucket, r, ec).map(_.headOption))
  }

  def tail[T](from: Long = 0L, every: Long = 1000L, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    trigger.map( _ => Couchbase.tailableQuery[JsObject](docName, viewName, { obj =>
      (obj \ cappedNaturalId).as[Long]
    }, from, every, unit)(bucket, CouchbaseRWImplicits.documentAsJsObjectReader, ec).through(Enumeratee.map { elem =>
       r.reads(elem.asInstanceOf[JsValue])
    }).through(Enumeratee.collect {
      case JsSuccess(elem, _) => elem
    }))
  }

  def insert[T](key: String, value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    val jsObj = w.writes(value).as[JsObject]
    val enhancedJsObj = jsObj ++ Json.obj(cappedRef -> true, cappedNaturalId -> System.currentTimeMillis())
    trigger.flatMap(_ => Couchbase.set[JsObject](key, enhancedJsObj, exp, persistTo, replicateTo)(bucket, CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }

  def insertWithKey[T](key: T => String, value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    val jsObj = w.writes(value).as[JsObject]
    val enhancedJsObj = jsObj ++ Json.obj(cappedRef -> true, cappedNaturalId -> System.currentTimeMillis())
    trigger.flatMap(_ => Couchbase.setWithKey[JsObject]({ _ => key(value)}, enhancedJsObj, exp, persistTo, replicateTo)(bucket, CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }

  def insertStream[T](data: Enumerator[(String, T)], exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    val enhancedEnumerator = data.through(Enumeratee.map { elem =>
      val jsObj = w.writes(elem._2).as[JsObject]
      val enhancedJsObj = jsObj ++ Json.obj(cappedRef -> true, cappedNaturalId -> System.currentTimeMillis())
      (elem._1, enhancedJsObj)
    })
    trigger.flatMap(_ => Couchbase.setStream[JsObject](enhancedEnumerator, exp, persistTo, replicateTo)(bucket, CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }

  def insertStreamWithKey[T](key: T => String, data: Enumerator[T], exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    val enhancedEnumerator = data.through(Enumeratee.map { elem =>
      val jsObj = w.writes(elem).as[JsObject]
      val enhancedJsObj = jsObj ++ Json.obj(cappedRef -> true, cappedNaturalId -> System.currentTimeMillis())
      (key(elem), enhancedJsObj)
    })
    trigger.flatMap(_ => Couchbase.setStream[JsObject](enhancedEnumerator, exp, persistTo, replicateTo)(bucket, CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }
}
