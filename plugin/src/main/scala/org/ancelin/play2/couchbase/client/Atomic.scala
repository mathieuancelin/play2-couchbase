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

    case "poke" => Logger.info("got a poke")
    case casr: CASResponse =>
      Logger.info("got a CASResponse"); sender ! casr
    case f: Future[Any] =>
      Logger.info("got a Future"); sender ! f
    case ar: AtomicRequest[T] => {
      Logger.info(">>>> " + ar.toString)
      implicit val rr = ar.r
      implicit val ww = ar.w
      implicit val bb = ar.bucket
      implicit val ee = ar.ec

      val sen = sender
      val myresult = ar.atomic.getAndLock(ar.key, 3600).onComplete {
        case Success(Some(cas)) => {
          if (cas.getCas.equals(-1)) {
            Logger.error("cas -1")

          }

          Logger.info("cas " + cas.toString())
          Logger.info("cas " + cas.getCas())
          Logger.info("cas " + cas.getValue())

          val cv = ar.r.reads(Json.parse(cas.getValue.toString)).get //Json.fromJson[T](Json.parse(cas.getValue.toString))  //.re
          val nv = ar.operation(cv)

          Logger.info("nv " + nv.toString())

          val res = ar.bucket.couchbaseClient.cas(ar.key, cas.getCas, ar.w.writes(nv).toString)
          Logger.info("res " + res.toString())

          sen ! res
        }
        //       case Failure(t) => {
        case _ => {
          Logger.error("""\!\!\!\!cannot update """ + ar.key)
          implicit val timeout = Timeout(180 seconds)
          val ar2 = AtomicRequest(ar.key, ar.operation, ar.bucket, ar.atomic, ar.r, ar.w, ar.ec, ar.numberTry + 1)
          val atomic_actor = Akka.system.actorOf(AtomicActor.props[T]) // , name = "atomic_update_" + UUID.randomUUID
         /* val delayed = after(1000 millis, using =
            context.system.scheduler)(Future.successful("time's up"))*/
          Logger.info("-----------------------------------plop")
            for(
                d <- after(2000 millis, using =
            context.system.scheduler)(Future.successful(ar2));
                tr <- (atomic_actor ? d)
                ) yield ( sen ! tr)          
            
//          Await.result(delayed, 2 minutes)
       //   sen ! Await.result(atomic_actor ? ar2, 2 minutes)

        }
      }
      Logger.info("____________::: " + myresult)
  //    sender ! myresult
    }
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

