package org.ancelin.play2.couchbase

import play.api.{Logger, Plugin, Application}
import com.typesafe.config.ConfigObject
import collection.JavaConversions._

class CouchbasePlugin(implicit app: Application) extends Plugin {
  val logger = Logger("CouchbasePlugin")
  var buckets: Map[String, CouchbaseBucket] = Map[String, CouchbaseBucket]()
  override def onStart {
    logger.info("Starting Couchbase plugin ...")
    play.api.Play.configuration(app).getObjectList("couchbase.buckets").map { configs =>
      configs.foreach(connect)
      configs
    }.getOrElse {
      logger.info(s"Connection to default CouchBase ...")
      val cl: CouchbaseBucket = Couchbase().connect()
      buckets = buckets + (cl.bucket -> cl)
    }
    logger.info("Starting Couchbase plugin done, have fun !!!")
  }
  private def connect(config: ConfigObject) {
    val bucket = config.get("bucket").unwrapped().asInstanceOf[String]
    val hosts = config.get("host").unwrapped() match {
      case s: String => List(s)
      case a: java.util.ArrayList[String] => a.toList
    }
    val port = config.get("port").unwrapped().asInstanceOf[String]
    val base = config.get("base").unwrapped().asInstanceOf[String]
    val user = config.get("user").unwrapped().asInstanceOf[String]
    val pass = config.get("pass").unwrapped().asInstanceOf[String]
    val timeout = config.get("timeout").unwrapped().asInstanceOf[String].toLong
    val couchbase: CouchbaseBucket = Couchbase(hosts.toList, port, base, bucket, user, pass, timeout)
    logger.info(s"""Connection to bucket "${bucket}" ...""")
    buckets = buckets + (bucket -> couchbase.connect())
  }
  override def onStop {
    logger.info("Couchbase plugin shutdown, disconnecting all buckets ...")
    buckets.foreach { tuple => tuple._2.disconnect() }
    buckets = buckets.empty
    logger.info("Couchbase plugin shutdown done.")
  }
}