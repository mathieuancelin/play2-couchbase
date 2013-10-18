package org.ancelin.play2.couchbase.client

import org.ancelin.play2.couchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import org.ancelin.play2.couchbase.client.CouchbaseFutures._

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Counter Operations
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
trait Counters {

  def incr(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncIncr(key, by), ec )
  }

  def incr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncIncr(key, by), ec )
  }

  def decr(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncDecr(key, by), ec )
  }

  def decr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncDecr(key, by), ec )
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
}
