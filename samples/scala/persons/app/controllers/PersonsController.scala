package controllers

import org.ancelin.play2.couchbase.crud.CouchbaseCrudSourceController
import models.Persons.fmt
import models.Person
import org.ancelin.play2.couchbase.Couchbase
import play.api.Play.current

object PersonsController extends CouchbaseCrudSourceController[Person] {
  val bucket = Couchbase.bucket("persons")
  override val defaultViewName = "by_person"
  override val defaultDesignDocname = "persons"
}