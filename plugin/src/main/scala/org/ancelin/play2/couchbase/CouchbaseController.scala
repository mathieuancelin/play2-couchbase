package org.ancelin.play2.couchbase

import scala.concurrent.Future
import play.api.mvc._
import play.api.Play.current

trait CouchbaseController { self: Controller =>

  def defaultBucket = Couchbase.defaultBucket(current)
  def defaultClient = defaultBucket.client
  def buckets = Couchbase.buckets

  def CouchbaseAction(block: CouchbaseBucket => Future[SimpleResult]) =
    Action.async {
      implicit val client = Couchbase.defaultBucket(current)
      block(client)
    }

  def CouchbaseAction(bucket: String)(block: CouchbaseBucket => Future[SimpleResult]) =
    Action.async {
      implicit val client = Couchbase.bucket(bucket)(current)
      block(client)
    }

  def CouchbaseReqAction(block: play.api.mvc.Request[AnyContent] => CouchbaseBucket => Future[SimpleResult]) =
    Action.async {request =>
      implicit val client = Couchbase.defaultBucket(current)
      block(request)(client)
    }

  def CouchbaseReqAction(bucket :String)(block: play.api.mvc.Request[AnyContent] => CouchbaseBucket => Future[SimpleResult]) =
    Action.async {request =>
      implicit val client = Couchbase.bucket(bucket)(current)
      block(request)(client)
    }
}
