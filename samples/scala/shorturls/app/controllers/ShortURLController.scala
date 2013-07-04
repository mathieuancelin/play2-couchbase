package controllers

import org.ancelin.play2.couchbase.crud.CouchbaseCrudSourceController
import models.ShortURL
import models.ShortURLs.fmt
import org.ancelin.play2.couchbase.Couchbase
import play.api.Play.current

object ShortURLController extends CouchbaseCrudSourceController[ShortURL] {
  val bucket = Couchbase.bucket("default")
  override val defaultViewName = "by_url"
  override val defaultDesignDocname = "shorturls"
}