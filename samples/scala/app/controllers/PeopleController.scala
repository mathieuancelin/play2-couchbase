package controllers

import org.ancelin.play2.couchbase.CouchbaseController
import play.api.mvc.{Action, Controller}
import java.util.UUID
import models.People

object PeopleController extends Controller with CouchbaseController {

  def index() = Action {
     Async{
       PeopleController.findAll().map(peoples => Ok(views.html.all(peoples)))
     }
  }

  def show(id: String) = Action {
    Async {
       PeopleController.findById(id).map { maybePeople =>
          maybePeople.map(people => Ok(views.html.show(people))).getOrElse(NotFound)
       }
    }
  }

  def create(name: String, surname: String) = Action {
    Async {
      val people = People(UUID.randomUUID().toString, name, surname)
      PeopleController.save(people).map { status =>
        index()
      }
    }
  }

  def delete(id: String) = Action {
    Async {
      PeopleController.remove(id).map { status =>
        index()
      }
    }
  }

}
