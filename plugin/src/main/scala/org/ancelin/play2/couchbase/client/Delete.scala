package org.ancelin.play2.couchbase.client

import org.ancelin.play2.couchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import org.ancelin.play2.couchbase.client.CouchbaseFutures._
import net.spy.memcached.{PersistTo, ReplicateTo}

trait Delete {

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Delete Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def delete(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(key), ec )
  }

  def delete(key: String, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(key, replicateTo), ec )
  }

  def delete(key: String, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(key, persistTo), ec )
  }

  def delete(key: String, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(key, persistTo, replicateTo), ec )
  }

  def delete[T <: {def id:String}](value: T)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(value.id), ec )
  }

  def delete[T <: {def id:String}](value: T, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(value.id, replicateTo), ec )
  }

  def delete[T <: {def id:String}](value: T, persistTo: PersistTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(value.id, persistTo), ec )
  }

  def delete[T <: {def id:String}](value: T, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(value.id, persistTo, replicateTo), ec )
  }
}
