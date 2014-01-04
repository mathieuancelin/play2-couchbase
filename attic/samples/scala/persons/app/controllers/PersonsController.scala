package controllers

import org.ancelin.play2.couchbase.crud.CouchbaseCrudSourceController
import models.Persons.fmt
import models.Person
import org.ancelin.play2.couchbase.Couchbase
import play.api.Play.current

object PersonsController extends CouchbaseCrudSourceController[Person] {
  def bucket = Couchbase.bucket("persons")
  override def defaultViewName = "by_person"
  override def defaultDesignDocname = "persons"
}