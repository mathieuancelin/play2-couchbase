package org.ancelin.play2.couchbase

import scala.concurrent.Future
import play.api.mvc._
import play.api.mvc.Results._
import play.api.Play.current
import com.couchbase.client.CouchbaseClient

trait CouchbaseController { self: Controller =>

  def bucket = Couchbase.currentCouchbase(current)
  def client = bucket.client

  def CouchbaseAction(block: CouchbaseClient => Future[Result]):EssentialAction = {
    Action {
      Async {
        implicit val client = Couchbase.currentCouchbase(current).client.get
        block(client)
      }
    }
  }

  // not really useful
  def CouchbaseAction(block: (play.api.mvc.Request[AnyContent], CouchbaseClient) => Future[Result]):EssentialAction = {
    Action { request =>
      Async {
        implicit val client = Couchbase.currentCouchbase(current).client.get
        block(request, client)
      }
    }
  }

  def CouchbaseReqAction(block: play.api.mvc.Request[AnyContent] => CouchbaseClient => Future[Result]):EssentialAction = {
    Action { request =>
      Async {
        implicit val client = Couchbase.currentCouchbase(current).client.get
        block(request)(client)
      }
    }
  }
}
