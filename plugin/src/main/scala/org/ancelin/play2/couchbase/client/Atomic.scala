package org.ancelin.play2.couchbase.client

import org.ancelin.play2.couchbase.CouchbaseBucket
import scala.concurrent.{ Future, ExecutionContext }
import net.spy.memcached.ops.OperationStatus
import org.ancelin.play2.couchbase.client.CouchbaseFutures._

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Counter Operations
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
trait Atomic {

  def getAndLock(key: String, exp: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus(bucket.couchbaseClient.asyncGetAndLock(key, exp), ec)
  }

  def unlock(key: String, casId: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus(bucket.couchbaseClient.asyncUnlock(key, casId), ec)
  }

}
