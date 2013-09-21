package controllers

import org.ancelin.play2.couchbase.CouchbaseController
import play.api.mvc.{Action, Controller}
import java.util.UUID
import models.People
import models.Peoples
import models.Peoples._
import scala.concurrent.Future
import play.api.libs.json._
import play.api.libs.iteratee.{Concurrent, Enumeratee, Enumerator}
import java.util.concurrent.TimeUnit
import play.api.libs.concurrent.Promise
import play.api.libs.EventSource

object PeopleController extends Controller with CouchbaseController {

  //val peopleEnumerator: Enumerator[List[People]] = Enumerator.generateM[List[People]](
  //  Promise.timeout(Some, 500, TimeUnit.MILLISECONDS).flatMap( n => Peoples.findAll().map( Option( _ ) ) )
  //)

  val broadcast = Concurrent.broadcast[List[People]]

  val peopleEnumerator = broadcast._1

  def updateClients() = {
    Peoples.findAll().map( broadcast._2.push( _ ) )
  }

  val jsonTransformer: Enumeratee[List[People], JsValue] = Enumeratee.map { list =>
    Json.obj( "peoples" -> list )
  }

  def index() = Action.async {
    Peoples.findAll().map(peoples => Ok(views.html.all(peoples)))
  }

  def peoples() = Action {
    Ok.feed( peopleEnumerator &> jsonTransformer &> EventSource() ).as( "text/event-stream" )
  }

  def show(id: String) = Action.async {
    Peoples.findById(id).map { maybePeople =>
      maybePeople.map( people => Ok( Peoples.peopleWriter.writes(people) ) ).getOrElse( NotFound )
    }
  }

  def create() = Action.async { implicit request =>
    Peoples.peopleForm.bindFromRequest.fold(
      errors => Future(BadRequest("Not good !!!!")),
      people => {
        val peopleWithID = people.copy(id = Some(UUID.randomUUID().toString))
        Peoples.save(peopleWithID).map { status =>
          updateClients()
          Ok( Json.obj( "success" -> status.isSuccess,"message" -> status.getMessage, "people" -> Peoples.peopleWriter.writes(peopleWithID) ) )
        }
      }
    )
  }

  def delete(id: String) = Action.async {
    Peoples.remove(id).map { status =>
      updateClients()
      Ok( Json.obj( "success" -> status.isSuccess,"message" -> status.getMessage) )
    }
  }
}
