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

object AtomicActor {
  def props[T](key: String, operation: T => T, bucket: CouchbaseBucket, atomic: Atomic, r: Reads[T], w: Writes[T], ec: ExecutionContext): Props = Props(classOf[AtomicActor[T]], key, operation, bucket, atomic, r, w, ec)
}

class AtomicActor[T](val key: String, val operation: T => T, bucket: CouchbaseBucket, atomic: Atomic, val r: Reads[T], val w: Writes[T], ec: ExecutionContext) extends Actor {
  implicit val rr = r
  implicit val ww = w
  implicit val bb = bucket
  implicit val ee = ec

  def receive = {
    case numberTry => {
      val y = atomic.getAndLock(key, 3600).map(_.fold({
        Logger.error("cannot update " + key)
        Akka.system.scheduler.scheduleOnce(2.seconds, self, "poke")
        "NO"
      })(cas => {
        Logger.info("cas " + cas.toString())

        val cv = r.reads(Json.parse(cas.getValue.toString)).get //Json.fromJson[T](Json.parse(cas.getValue.toString))  //.re
        val nv = operation(cv)

        Logger.info("nv " + nv.toString())

        val res = bucket.couchbaseClient.cas(key, cas.getCas, w.writes(nv).toString)
        Logger.info("res " + res.toString())

        "YES"
      }))
      if(y.eq("YES")){
        sender ! "YES"
      }
    }
    //sender ! "poker " + key + " " + numberTry
  }
}

trait Atomic {

  def getAndLock[T](key: String, exp: Int)(implicit r: Reads[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[CASValue[T]]] = {
    // waitForOperationStatus(bucket.couchbaseClient.asyncGetAndLock(key, exp), ec)
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
    // val atomic_actor = Akka.system.actorOf(Props[AtomicActor])// , name = "atomic_update_" + UUID.randomUUID
    val atomic_actor = Akka.system.actorOf(AtomicActor.props[T](key, operation, bucket, this, r, w, ec)) // , name = "atomic_update_" + UUID.randomUUID
    (atomic_actor.ask(1)) //.mapTo[String] //.mapTo[T]
  }

}

