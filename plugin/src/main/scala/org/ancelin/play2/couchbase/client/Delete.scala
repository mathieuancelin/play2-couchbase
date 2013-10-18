package org.ancelin.play2.couchbase.client

import org.ancelin.play2.couchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import org.ancelin.play2.couchbase.client.CouchbaseFutures._
import net.spy.memcached.{PersistTo, ReplicateTo}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Delete Operations
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
trait Delete {

  def delete(key: String, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(key, persistTo, replicateTo), ec )
  }

  def delete[T <: {def id:String}](value: T, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(value.id, persistTo, replicateTo), ec )
  }
}
