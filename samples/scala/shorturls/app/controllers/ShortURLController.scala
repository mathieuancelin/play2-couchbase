package controllers

import org.ancelin.play2.couchbase.crud.CouchbaseCrudSourceController
import models.ShortURL
import models.ShortURLs.fmt
import org.ancelin.play2.couchbase.Couchbase
import play.api.Play.current

object ShortURLController extends CouchbaseCrudSourceController[ShortURL] {
  val bucket = Couchbase.bucket("default")
  override def defaultViewName = "by_url"
  override def defaultDesignDocname = "shorturls"
  override def idKey = "id"
}