package org.ancelin.play2.couchbase

import scala.concurrent.Future
import play.api.mvc._
import play.api.Play.current
import com.couchbase.client.CouchbaseClient

trait CouchbaseController { self: Controller =>

  def defaultBucket = Couchbase.defaultBucket(current)
  def defaultClient = defaultBucket.client
  def buckets = Couchbase.buckets

  def CouchbaseAction(block: CouchbaseClient => Future[Result]):EssentialAction = {
    Action {
      Async {
        implicit val client = Couchbase.defaultBucket(current).client.get
        block(client)
      }
    }
  }

  def CouchbaseAction(bucket :String)(block: CouchbaseClient => Future[Result]):EssentialAction = {
    Action {
      Async {
        implicit val client = Couchbase.bucket(bucket)(current).client.get
        block(client)
      }
    }
  }

  def CouchbaseReqAction(block: play.api.mvc.Request[AnyContent] => CouchbaseClient => Future[Result]):EssentialAction = {
    Action { request =>
      Async {
        implicit val client = Couchbase.defaultBucket(current).client.get
        block(request)(client)
      }
    }
  }

  def CouchbaseReqAction(bucket :String)(block: play.api.mvc.Request[AnyContent] => CouchbaseClient => Future[Result]):EssentialAction = {
    Action { request =>
      Async {
        implicit val client = Couchbase.bucket(bucket)(current).client.get
        block(request)(client)
      }
    }
  }
}
