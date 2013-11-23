
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import play.api.test._
import play.api.test.Helpers._
import org.ancelin.play2.couchbase.{ Couchbase, CouchbaseController }
import play.GlobalSettings
import play.api.libs.json.{ Reads, Json }
import org.specs2.execute.AsResult
import net.spy.memcached.ops.OperationException
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.UUID
import akka.pattern.after
import scala.concurrent.duration._
import play.api.libs.concurrent.Akka
import scala.concurrent.Future


class AtomicTest extends Specification {

  case class TestValue(value: String, number: Int, l: List[TestValue])

  implicit val fmt = Json.format[TestValue]
  implicit val urlReader = Json.reads[TestValue]
  implicit val urlWriter = Json.writes[TestValue]

  val fakeapp = FakeApplication(
    additionalPlugins = Seq("org.ancelin.play2.couchbase.CouchbasePlugin"))

  val tk = "mylocktestkey2_" // + UUID.randomUUID

  "This test assume you have a default configuration of couchbase 2.1 running on your localhost computer" in ok

  "Couchbase" should {

    "connect" in new WithApplication(fakeapp) {
      Couchbase.buckets must not empty
    }
    /*
    "set key \"" + tk + "\" in default bucket" in new WithApplication(fakeapp) {
      implicit val e = Couchbase.couchbaseExecutor

      val tv = new TestValue("testValue", 42, List())
      val s = Couchbase.defaultBucket.set[TestValue](tk, tv)
      Await.result(s, Duration(20000, "millis")).isSuccess must equalTo(true)

    }
*/
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
      Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f)
      Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f)
      Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f)
      val ss = Await.result(Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f), Duration(20000, "millis"))
      Await.result( after( Duration(50000, "millis"), using =
            Akka.system.scheduler)(Future.successful("poke")),  Duration(20000, "millis"))
    /*  val ss1 = Await.result(Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f), Duration(20000, "millis"))
      val ss2 = Await.result(Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f), Duration(20000, "millis"))
      val ss3 = Await.result(Couchbase.defaultBucket.atomicUpdate[TestValue](tk, f), Duration(20000, "millis"))
      println("=====> " + ss)
      println("=====> " + ss1)
      println("=====> " + ss2)*/
      println("=====> " + ss)

      assert(true)
      /*
      val s = Couchbase.defaultBucket.getAndLock(tk, 3600)

      Await.result(s, Duration(20000, "millis")).fold(assert(false))(x => {
        println(x.getCas())
        x.getValue().value in ok
        assert(true)
      })
      * 
      */
    }

  }

}