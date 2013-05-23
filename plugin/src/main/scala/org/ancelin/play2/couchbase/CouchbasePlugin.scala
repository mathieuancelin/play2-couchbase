package org.ancelin.play2.couchbase

import play.api.{Logger, Plugin, Application}

class CouchbasePlugin(implicit app: Application) extends Plugin {
  val logger = Logger("CouchbasePlugin")
  var defaultCouch: Option[Couchbase] = None
  override def onStart {
    logger.info("Connection to CouchBase ...")
    defaultCouch = Option(Couchbase().connect())
    logger.info("Connected !!!")
  }
  override def onStop {
    logger.info("Couchbase shutdown")
    defaultCouch = Option(Couchbase().disconnect())
  }
}
