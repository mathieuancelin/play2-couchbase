package org.ancelin.play2.couchbase.client

import org.ancelin.play2.couchbase.CouchbaseBucket
import scala.concurrent.{ Future, ExecutionContext }
import net.spy.memcached.ops.OperationStatus
import org.ancelin.play2.couchbase.client.CouchbaseFutures._
import net.spy.memcached.transcoders.Transcoder
import net.spy.memcached.CASValue
import collection.JavaConversions._
import play.api.libs.json.Reads


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Counter Operations
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
trait Atomic {

  def getAndLock[T](key: String, exp: Int)(implicit r:Reads[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[CASValue[T]]] = {
    // waitForOperationStatus(bucket.couchbaseClient.asyncGetAndLock(key, exp), ec)
    waitForGetAndCas[T](bucket.couchbaseClient.asyncGetAndLock(key, exp), ec, r) map {
      case value: CASValue[T] =>
        println(value); Some[CASValue[T]](value)
      case _ => None
    }
  }

  def unlock(key: String, casId: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus(bucket.couchbaseClient.asyncUnlock(key, casId), ec)
  }

}

