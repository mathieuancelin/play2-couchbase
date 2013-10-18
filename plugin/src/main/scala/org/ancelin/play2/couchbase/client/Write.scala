package org.ancelin.play2.couchbase.client

import org.ancelin.play2.couchbase.CouchbaseBucket
import play.api.libs.json.{Json, Writes}
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import net.spy.memcached.{PersistTo, ReplicateTo}
import net.spy.memcached.transcoders.Transcoder
import org.ancelin.play2.couchbase.client.CouchbaseFutures._

trait Write {

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
    waitForOperationStatus( bucket.couchbaseClient.set(key, Constants.expiration, value, tc), ec )
  }

  def set[T](key: String, exp: Int, value: T, tc: Transcoder[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, value, tc), ec )
  }

  def set[T](key: String, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, Constants.expiration, value)(bucket, w, ec)
  }

  def set[T](key: String, exp: Int, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value))), ec )
  }

  def set[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value)), replicateTo), ec )
  }

  def set[T](key: String, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, Constants.expiration, value, replicateTo)(bucket, w, ec)
  }

  def set[T](key: String, exp: Int, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value)), persistTo), ec )
  }

  def set[T](key: String, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, Constants.expiration, value, persistTo)(bucket, w, ec)
  }

  def set[T](key: String, exp: Int, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), ec )
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
    waitForOperationStatus( bucket.couchbaseClient.add(key, exp, Json.stringify(w.writes(value))), ec )
  }

  def add[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.add(key, exp, Json.stringify(w.writes(value)), replicateTo), ec )
  }

  def add[T](key: String, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, Constants.expiration, value, replicateTo)(bucket, w, ec)
  }

  def add[T](key: String, exp: Int, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.add(key, exp, Json.stringify(w.writes(value)), persistTo), ec )
  }

  def add[T](key: String, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, Constants.expiration, value, persistTo)(bucket, w, ec)
  }

  def add[T](key: String, exp: Int, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.add(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), ec )
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
    waitForOperationStatus( bucket.couchbaseClient.replace(key, exp, Json.stringify(w.writes(value))), ec )
  }

  def replace[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.replace(key, exp, Json.stringify(w.writes(value)), replicateTo), ec )
  }

  def replace[T](key: String, value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, Constants.expiration, value, replicateTo)(bucket, w, ec)
  }

  def replace[T](key: String, exp: Int, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.replace(key, exp, Json.stringify(w.writes(value)), persistTo), ec )
  }

  def replace[T](key: String, value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, Constants.expiration, value, persistTo)(bucket, w, ec)
  }

  def replace[T](key: String, exp: Int, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.replace(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), ec )
  }

  def replace[T](key: String, value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, Constants.expiration, value, persistTo,replicateTo)(bucket, w, ec)
  }

  def replace[T](key: String, value: T)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, Constants.expiration, value)(bucket, w, ec)
  }
}
