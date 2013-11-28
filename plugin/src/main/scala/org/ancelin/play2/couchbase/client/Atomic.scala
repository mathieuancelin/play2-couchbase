package org.ancelin.play2.couchbase.client

import org.ancelin.play2.couchbase.{ CouchbaseBucket, Couchbase }
import scala.concurrent.{ Future, ExecutionContext }
import net.spy.memcached.ops.OperationStatus
import org.ancelin.play2.couchbase.client.CouchbaseFutures._
import net.spy.memcached.CASValue
import play.api.libs.json._
import akka.actor.Actor
import play.libs.Akka
import akka.actor.Props
import java.util.UUID
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import play.api.Play.current
import play.Logger
import net.spy.memcached.CASResponse
import scala.util.Success
import scala.util.Failure
import akka.pattern.after
import scala.concurrent.Await
import sun.org.mozilla.javascript.internal.ast.Yield

case class AtomicRequest[T](key: String, operation: T => T, bucket: CouchbaseBucket, atomic: Atomic, r: Reads[T], w: Writes[T], ec: ExecutionContext, numberTry: Int)

object AtomicActor {
  def props[T]: Props = Props(classOf[AtomicActor[T]])
}

class AtomicActor[T] extends Actor {

  def receive = {

    case ar: AtomicRequest[T] => {
      // I need some implicit, I know it's not good looking
      implicit val rr = ar.r
      implicit val ww = ar.w
      implicit val bb = ar.bucket
      implicit val ee = ar.ec
      // backup my sender, need it later, and actor shared state is not helping...
      val sen = sender
      val myresult = ar.atomic.getAndLock(ar.key, 3600).onComplete {
        case Success(Some(cas)) => {
          // \o/ we successfully lock the key
          // get current object
          val cv = ar.r.reads(Json.parse(cas.getValue.toString)).get
          // apply transformation and get new object
          val nv = ar.operation(cv)
          // write new object to couchbase and unlock :-)
          // TODO : use asyncCAS method, better io management
          val res = ar.bucket.couchbaseClient.cas(ar.key, cas.getCas, ar.w.writes(nv).toString)
          // reply to sender it's OK
          sen ! res
        }
        case _ => {
          // Too bad, the object is not locked...
          // first define implicit
          implicit val timeout = Timeout(180 seconds)
          // build a new atomic request
          val ar2 = AtomicRequest(ar.key, ar.operation, ar.bucket, ar.atomic, ar.r, ar.w, ar.ec, ar.numberTry + 1)
          // get my actor
          val atomic_actor = Akka.system.actorOf(AtomicActor.props[T])
          // wait and retry by asking actor with new atomic request
          for (
            d <- after(200 millis, using =
              context.system.scheduler)(Future.successful(ar2));
            tr <- (atomic_actor ? d)
          // send result :-)
          ) yield (sen ! tr)

        }
      }
    }
    case _ => Logger.error("An atomic actor get a message, but not a atomic request, it's weird ! ")
  }
}

trait Atomic {

  def getAndLock[T](key: String, exp: Int)(implicit r: Reads[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[CASValue[T]]] = {
    waitForGetAndCas[T](bucket.couchbaseClient.asyncGetAndLock(key, exp), ec, r) map {
      case value: CASValue[T] =>
        Some[CASValue[T]](value)
      case _ => None
    }
  }

  def unlock(key: String, casId: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus(bucket.couchbaseClient.asyncUnlock(key, casId), ec)
  }

  def atomicUpdate[T](key: String, operation: T => T)(implicit bucket: CouchbaseBucket, ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[Any] = {
    implicit val timeout = Timeout(180 seconds)
    val ar = AtomicRequest[T](key, operation, bucket, this, r, w, ec, 1)
    val atomic_actor = Akka.system.actorOf(AtomicActor.props[T])
    (atomic_actor.ask(ar))
  }

}

