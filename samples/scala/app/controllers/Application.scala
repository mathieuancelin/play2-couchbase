package controllers

import play.api.mvc.{Action, Controller}
import play.api.libs.json._
import org.ancelin.play2.couchbase.CouchbaseReads._
import org.ancelin.play2.couchbase.Couchbase._
import org.ancelin.play2.couchbase.Couchbase
import com.couchbase.client.protocol.views.{ComplexKey, Query}
import org.ancelin.play2.couchbase.CouchbaseController
import play.api.Play.current

//import play.api.libs.json.Reads._
//import play.api.libs.json.Writes._

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
    Ok(views.html.index("Hello World!"))
  }

  def getContent(key: String) = CouchbaseReqAction { implicit request => implicit client =>
    get[String](key).map { opt =>
      opt.map(Ok(_)).getOrElse(BadRequest(s"Unable to find content with key: $key"))
    }
  }

  def getPerson(key: String) = CouchbaseAction("default") { implicit client =>
    get[Person](key).map { opt =>
      opt.map(person => Ok(person.toString)).getOrElse(BadRequest(s"Unable to find person with key: $key"))
    }
  }

  def create() = CouchbaseAction { implicit client =>
    val jane = Person("Jane", "Doe")
    val json = Json.obj("name" -> "Bob", "surname" -> "Bob")
    for {
      _ <- delete("bob")
      _ <- delete("jane")
      f1 <- add[JsObject]("bob", json)
      f2 <- add[Person]("jane", jane)
    } yield Ok("bob: " +f1.getMessage + "<br/>jane: " + f2.getMessage)
  }

  def query() = CouchbaseAction { implicit client =>
    //val findBeerByName = find[Beer]("beer", "by_name")
    val query = new Query().setIncludeDocs(true)
      .setLimit(20)
      .setRangeStart(ComplexKey.of("(512)"))
      .setRangeEnd(ComplexKey.of("(512)" + "\uefff"))
    /*findBeerByName(query).map { list =>
      Ok(list.map(_.toString).mkString("\n"))
    }*/
    find[Beer]("beer", "by_name")(query).map { list =>
      Ok(list.map(_.toString).mkString("\n"))
    }
  }
}