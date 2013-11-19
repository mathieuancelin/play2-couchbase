
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import play.api.test._
import play.api.test.Helpers._
import org.ancelin.play2.couchbase.{ Couchbase, CouchbaseController }
import play.GlobalSettings


@RunWith(classOf[JUnitRunner])
class AtomicTest extends Specification {

  val fakeapp = FakeApplication(
    additionalPlugins = Seq("org.ancelin.play2.couchbase.CouchbasePlugin"))

  "This test assume you have a default configuration of couchbase 2.1 running on your localhost computer" in ok

  "Couchbase" should {

    "connect" in new WithApplication(fakeapp) {
      Couchbase.buckets must not empty
    }
  }

}