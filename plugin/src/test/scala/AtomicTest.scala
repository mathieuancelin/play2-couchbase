
import org.specs2.mutable._
import org.junit.runner._
import play.api.test._
import org.ancelin.play2.couchbase.{ Couchbase, CouchbaseController }
import play.GlobalSettings
import play.api.libs.json.{ Reads, Json }
import org.specs2.execute.AsResult
import net.spy.memcached.ops.OperationException
import scala.concurrent.Await
import java.util.UUID
import akka.pattern.after
import scala.concurrent.duration._
import play.api.libs.concurrent.Akka
import scala.concurrent.Future
import scala.util.Success
import scala.util.Failure
import org.ancelin.play2.couchbase.CouchbaseExpiration._


class AtomicTest extends Specification {

  case class TestValue(value: String, number: Int, l: List[TestValue])

  implicit val fmt = Json.format[TestValue]
  implicit val urlReader = Json.reads[TestValue]
  implicit val urlWriter = Json.writes[TestValue]

  val fakeapp = FakeApplication(
    additionalPlugins = Seq("org.ancelin.play2.couchbase.CouchbasePlugin"))

  val tk = "mylocktestkey_" // + UUID.randomUUID

  "This test assume you have a default configuration of couchbase 2.1 running on your localhost computer" in ok

  "Couchbase" should {

    "connect" in new WithApplication(fakeapp) {
      Couchbase.buckets must not empty
    }

    "set key \"" + tk + "\" in default bucket" in new WithApplication(fakeapp) {
      implicit val e = Couchbase.couchbaseExecutor

      val tv = new TestValue("testValue", 42, List())
      val s = Couchbase.defaultBucket.set[TestValue](tk, tv, Duration(2, "hours"))
      Await.result(s, Duration(20000, "millis")).isSuccess must equalTo(true)

    }

    "lock the key \"" + tk + "\" in default bucket" in new WithApplication(fakeapp) {

      implicit val e = Couchbase.couchbaseExecutor

      val f = { x: TestValue =>
        {
          val l = x.l.toList
          val ll = (new TestValue("testValue", l.size, List())) :: l
          val ntv = new TestValue(x.value, x.number, ll)
          ntv
        }
      }
      Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f).onComplete({
        case Success(r) => "Success of 1 atomic update" in ok
        case Failure(r) => "Faillure of 1 atomic update" in failure
      })
      Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f).onComplete({
        case Success(r) => "Success of 2 atomic update" in ok
        case Failure(r) => "Faillure of 2 atomic update" in failure
      })
      Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f).onComplete({
        case Success(r) => "Success of 3 atomic update" in ok
        case Failure(r) => "Faillure of 3 atomic update" in failure
      })
      Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f).onComplete({
        case Success(r) =>"Success of 4 atomic update" in ok
        case Failure(r) => "Faillure of 4 atomic update" in failure
      })

      Couchbase.defaultBucket.atomicUpdate[TestValue](tk + "-plop", f).onComplete({
        case Success(r) => "Failure : successfully updated a non existing key O_o" in failure
        case Failure(r) => "Success : unable to update a non existing key" in ok
      })

      Await.result(after(Duration(6000, "millis"), using =
        Akka.system.scheduler)(Future.successful("poke")), Duration(7001, "millis"))


    }

  }

}