package test

import org.specs2.mutable._

import play.api.test._
import play.api.test.Helpers._
import play.api.test.FakeApplication
import java.util.concurrent.{Executors, CountDownLatch}
import play.api.libs.iteratee
import scala.concurrent.ExecutionContext

/**
 * Add your spec here.
 * You can mock out a whole application including requests, plugins etc.
 * For more information, consult the wiki.
 */
class ApplicationSpec extends Specification {

  "Application" should {

    val app = FakeApplication()
    implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

    "find an some urls" in {
      running(app) {
        println("=======================================================>   test1 ================================================")
        val home = route(app, FakeRequest(GET, "/urls/")).get
        val content = contentAsString(home)
        println(content)
        status(home) must equalTo(OK)


        val home2 = route(app, FakeRequest(GET, "/urls/")).get
        val content2 = contentAsString(home2)
        println(content2)
        status(home2) must equalTo(OK)

        println("=======================================================>   fin test1 ================================================")
        contentType(home) must beSome.which(_ == "application/json")
      }
    }

    "find other urls" in {
      running(app) {
        val latch = new CountDownLatch(1)
        println("=======================================================>   test2 l1  ================================================")
        route(app, FakeRequest(GET, "/urls/")).get.map { home =>
          home.body |>>> iteratee.Iteratee.consume[Array[Byte]]().map { content =>
            println("=======================================================>   test2 l2  ================================================")
            println(content)
            println("=======================================================>   test2 l3  ================================================")
            latch.countDown()
            println("=======================================================>   test2 l5  ================================================")
          }
        }
        latch.await()
        println("=======================================================>   fin test2   ================================================")
        contentType(route(app, FakeRequest(GET, "/urls/")).get) must beSome.which(_ == "application/json")
      }
    }
  }
}