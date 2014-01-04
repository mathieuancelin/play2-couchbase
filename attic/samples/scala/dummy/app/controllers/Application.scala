package controllers

import play.api.mvc.{Action, Controller}
import play.api.libs.json._
import org.ancelin.play2.couchbase.CouchbaseRWImplicits._
import org.ancelin.play2.couchbase.Couchbase
import com.couchbase.client.protocol.views.{ComplexKey, Query}
import org.ancelin.play2.couchbase.CouchbaseController
import play.api.Play.current

case class Person(name: String, surname: String)
case class Beer(name: String, code: String)

object Application extends Controller with CouchbaseController {

  implicit val customExecutionContext = Couchbase.couchbaseExecutor
  implicit val personReader = Json.reads[Person]
  implicit val personWriter = Json.writes[Person]
  implicit val beerReader = new Reads[Beer] {
    def reads(json: JsValue): JsResult[Beer] = {
      val name = (json \ "name").as[String]
      val code = (json \ "brewery_id").as[String]
      JsSuccess(Beer(name, code))
    }
  }

  def index() = Action {
    Redirect( routes.PeopleController.index() )
  }

  def dummyIndex() = Action {
    Ok(views.html.index("Hello World!"))
  }

  def getContent(key: String) = CouchbaseReqAction { implicit request => bucket =>
    bucket.get[String](key).map { opt =>
      opt.map(Ok(_)).getOrElse(BadRequest(s"Unable to find content with key: $key"))
    }
  }

  def getPerson(key: String) = CouchbaseAction("default") { implicit bucket =>
    bucket.get[Person](key).map { opt =>
      opt.map(person => Ok(person.toString)).getOrElse(BadRequest(s"Unable to find person with key: $key"))
    }
  }

  def create() = CouchbaseAction { implicit bucket =>
    val jane = Person("Jane", "Doe")
    val json = Json.obj("name" -> "Bob", "surname" -> "Bob")
    for {
      _ <- bucket.delete("bob")
      _ <- bucket.delete("jane")
      f1 <- bucket.add[JsObject]("bob", json)
      f2 <- bucket.add[Person]("jane", jane)
    } yield Ok("bob: " +f1.getMessage + "<br/>jane: " + f2.getMessage)
  }

  def query() = CouchbaseAction { implicit bucket =>
    //val findBeerByName = find[Beer]("beer", "by_name")
    val query = new Query().setIncludeDocs(true)
      .setLimit(20)
      .setRangeStart(ComplexKey.of("(512)"))
      .setRangeEnd(ComplexKey.of("(512)" + "\uefff"))
    /*findBeerByName(query).map { list =>
      Ok(list.map(_.toString).mkString("\n"))
    }*/
    bucket.find[Beer]("beer", "by_name")(query).map { list =>
      Ok(list.map(_.toString).mkString("\n"))
    }
  }
}