package org.ancelin.play2.couchbase

import play.api.libs.json._
import scala.concurrent.{Future, ExecutionContext}
import com.couchbase.client.protocol.views.{DesignDocument, SpatialView, View, Query}
import play.api.libs.iteratee.Enumerator
import net.spy.memcached.ops.OperationStatus
import play.api.libs.json.JsObject
import net.spy.memcached.{PersistTo, ReplicateTo}

trait BucketAPI {
    self: CouchbaseBucket =>

  /**
  def findP[T](docName:String, viewName: String)(implicit r: Reads[T], ec: ExecutionContext): PartialFunction[Query, Future[List[T]]] = {
    Couchbase.findP[T](docName, viewName)(self, r, ec)
  }

  def findP[T](view: View)(implicit r: Reads[T], ec: ExecutionContext): PartialFunction[Query, Future[List[T]]] = {
    Couchbase.findP[T](view)(self, r, ec)
  }

  def findPAsEnumerator[T](docName:String, viewName: String)(implicit r: Reads[T], ec: ExecutionContext): PartialFunction[Query, Future[Enumerator[T]]] = {
    Couchbase.findPAsEnumerator[T](docName, viewName)(self, r, ec)
  }

  def findPAsEnumerator[T](view: View)(implicit r: Reads[T], ec: ExecutionContext): PartialFunction[Query, Future[Enumerator[T]]] = {
    Couchbase.findPAsEnumerator[T](view)(self, r, ec)
  }
  **/

  def find[T](docName:String, viewName: String)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Couchbase.find[T](docName, viewName)(query)(self, r, ec)
  }

  def find[T](view: View)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Couchbase.find[T](view)(query)(self, r, ec)
  }

  def findAsEnumerator[T](view: View)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    Couchbase.findAsEnumerator[T](view)(query)(self, r, ec)
  }

  def findAsEnumerator[T](docName:String, viewName: String)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    Couchbase.findAsEnumerator[T](docName, viewName)(query)(self, r, ec)
  }

  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long, filter: T => Boolean = { chunk: T => true })(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Couchbase.pollQuery[T](doc, view, query, everyMillis, filter)(self, r, ec)
  }

  def repeatQuery[T](doc: String, view: String, query: Query, filter: T => Boolean = { chunk: T => true }, trigger: Future[AnyRef] = Future.successful(Some))(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Couchbase.repeatQuery[T](doc, view, query, trigger, filter)(self, r, ec)
  }

  def view(docName: String, viewName: String)(implicit ec: ExecutionContext): Future[View] = {
    Couchbase.view(docName, viewName)(self, ec)
  }

  def spatialView(docName: String, viewName: String)(implicit ec: ExecutionContext): Future[SpatialView] = {
    Couchbase.spatialView(docName, viewName)(self, ec)
  }

  def designDocument(docName: String)(implicit ec: ExecutionContext): Future[DesignDocument[_]] = {
    Couchbase.designDocument(docName)(self, ec)
  }

  def createDesignDoc(name: String, value: JsObject)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.createDesignDoc(name, value)(self, ec)
  }

  def createDesignDoc(name: String, value: String)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.createDesignDoc(name, value)(self, ec)
  }

  def createDesignDoc(value: DesignDocument[_])(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.createDesignDoc(value)(self, ec)
  }

  def deleteDesignDoc(name: String)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.deleteDesignDoc(name)(self, ec)
  }

  def keyStats(key: String)(implicit ec: ExecutionContext): Future[Map[String, String]] = {
    Couchbase.keyStats(key)(self, ec)
  }

  def get[T](key: String)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    Couchbase.get[T](key)(self, r, ec)
  }

  def getBulk[T](keys: Seq[String])(implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Couchbase.getBulk[T](keys)(self, r, ec)
  }

  def getBulk[T](keys: Enumerator[String])(implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Couchbase.getBulk[T](keys)(self, r, ec)
  }

  def getBulkWithKeys[T](keys: Enumerator[String])(implicit r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    Couchbase.getBulkWithKeys[T](keys)(self, r, ec)
  }

  def getBulkWithKeys[T](keys: Seq[String])(implicit r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    Couchbase.getBulkWithKeys[T](keys)(self, r, ec)
  }

  def getBulkAsEnumerator[T](keys: Enumerator[String])(implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    Couchbase.getBulkAsEnumerator[T](keys)(self, r, ec)
  }

  def getBulkAsEnumerator[T](keys: Seq[String])(implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    Couchbase.getBulkAsEnumerator[T](keys)(self, r, ec)
  }

  def getBulkWithKeysAsEnumerator[T](keys: Seq[String])(implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[(String, T)]] = {
    Couchbase.getBulkWithKeysAsEnumerator[T](keys)(self, r, ec)
  }

  def getBulkWithKeysAsEnumerator[T](keys: Enumerator[String])(implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[(String, T)]] = {
    Couchbase.getBulkWithKeysAsEnumerator[T](keys)(self, r, ec)
  }

  def incr(key: String, by: Int)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.incr(key, by)(self, ec)
  def incr(key: String, by: Long)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.incr(key, by)(self, ec)
  def decr(key: String, by: Int)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.decr(key, by)(self, ec)
  def decr(key: String, by: Long)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.decr(key, by)(self, ec)
  def incrAndGet(key: String, by: Int)(implicit ec: ExecutionContext): Future[Int] = Couchbase.incrAndGet(key, by)(self, ec)
  def incrAndGet(key: String, by: Long)(implicit ec: ExecutionContext): Future[Long] = Couchbase.incrAndGet(key, by)(self, ec)
  def decrAndGet(key: String, by: Int)(implicit ec: ExecutionContext): Future[Int] = Couchbase.decrAndGet(key, by)(self, ec)
  def decrAndGet(key: String, by: Long)(implicit ec: ExecutionContext): Future[Long] = Couchbase.decrAndGet(key, by)(self, ec)

  def set[T](key: String, value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.set[T](key, exp, value, persistTo, replicateTo)(self, w, ec)
  }

  def setWithKey[T](key: T => String, value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.setWithKey[T](value, key, exp, persistTo, replicateTo)(self, w, ec)
  }

  def setWithId[T <: {def id: String}](value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.set[T](value.id, exp, value, persistTo, replicateTo)(self, w, ec)
  }

  def add[T](key: String, value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.add[T](key, exp, value, persistTo, replicateTo)(self, w, ec)
  }

  def addWithKey[T](key: T => String, value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.addWithKey[T](value, key, persistTo, replicateTo)(self, w, ec)
  }

  def addWithId[T <: {def id: String}](value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.add[T](value.id, exp, value, persistTo, replicateTo)(self, w, ec)
  }

  def replace[T](key: String, value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.replace[T](key, exp, value, persistTo, replicateTo)(self, w, ec)
  }

  def replaceWithKey[T](key: T => String, value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.replaceWithKey[T](value, key, exp, persistTo, replicateTo)(self, w, ec)
  }

  def replaceWithId[T <: {def id: String}](value: T, exp: Int = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.replace[T](value.id, exp, value, persistTo, replicateTo)(self, w, ec)
  }

  def delete(key: String, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.delete(key, persistTo, replicateTo)(self, ec)
  }

  def deleteWithId[T <: {def id: String}](value: T, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.delete(value.id, persistTo, replicateTo)(self, ec)
  }

  def flush(delay: Int)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.flush(delay)(self, ec)
  def flush()(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.flush()(self, ec)
}
