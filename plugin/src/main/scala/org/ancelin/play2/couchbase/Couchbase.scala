package org.ancelin.play2.couchbase

import com.couchbase.client.CouchbaseClient
import java.net.URI
import java.util.concurrent.TimeUnit
import play.api.{Play, PlayException, Application}
import collection.JavaConversions._
import collection.mutable.ArrayBuffer
import play.api.Play.current
import scala.Some
import scala.concurrent.ExecutionContext
import play.api.libs.concurrent.Akka
import akka.actor.ActorSystem

class Couchbase(val client: Option[CouchbaseClient], val host: String, val port: String, val base: String, val bucket: String, val pass: String, val timeout: Long) {

  def connect() = {
    val uris = ArrayBuffer(URI.create(s"http://$host:$port/$base"))
    val client = new CouchbaseClient(uris, bucket, pass)
    new Couchbase(Some(client), host, port, base, bucket, pass, timeout)
  }

  def disconnect() = {
    client.map(_.shutdown(timeout, TimeUnit.SECONDS))
    new Couchbase(None, host, port, base, bucket, pass, timeout)
  }

  def withCouchbase[T](block: CouchbaseClient => T): Option[T] = {
    client.map(block(_))
  }
}

object Couchbase extends ClientWrapper {

  private val initMessage = "The CouchbasePlugin has not been initialized! Please edit your conf/play.plugins file and add the following line: '400:package org.ancelin.play2.couchbase.CouchbasePlugin' (400 is an arbitrary priority and may be changed to match your needs)."
  private val connectMessage = "The CouchbasePlugin doesn't seems to be connected to a Couchbase server. Maybe an error occured!"
  private val couchbaseActorSystem = ActorSystem("couchbase-plugin-system")

  def defaultBucket(implicit app: Application): Couchbase = app.plugin[CouchbasePlugin] match {
    case Some(plugin) => plugin.buckets.headOption.getOrElse(throw new PlayException("CouchbasePlugin Error", connectMessage))._2
    case _ => throw new PlayException("CouchbasePlugin Error", initMessage)
  }

  def bucket(bucket: String)(implicit app: Application): Couchbase = buckets(app).get(bucket).getOrElse(throw new PlayException(s"Error with bucket $bucket", s"Bucket '$bucket' is not defined"))
  def client(bucket: String)(implicit app: Application): CouchbaseClient = buckets(app).get(bucket).flatMap(_.client).getOrElse(throw new PlayException(s"Error with bucket $bucket", s"Bucket '$bucket' is not defined or client is not connected"))

  def buckets(implicit app: Application): Map[String, Couchbase] =  app.plugin[CouchbasePlugin] match {
    case Some(plugin) => plugin.buckets
    case _ => throw new PlayException("CouchbasePlugin Error", initMessage)
  }


  def couchbaseExecutor(implicit app: Application): ExecutionContext = {
    app.configuration.getObject("couchbase-ec.execution-context") match {
      case Some(_) => couchbaseActorSystem.dispatchers.lookup("couchbase-ec.execution-context")
      case _ => throw new PlayException("Configuration issue","You have to define a 'couchbase-ec.execution-context' object in the application.conf file.")
    }
  }

  def apply(
             host:    String = Play.configuration.getString("couchbase.host").getOrElse("127.0.0.1"),
             port:    String = Play.configuration.getString("couchbase.port").getOrElse("8091"),
             base:    String = Play.configuration.getString("couchbase.base").getOrElse("pools"),
             bucket:  String = Play.configuration.getString("couchbase.bucket").getOrElse("default"),
             pass:    String = Play.configuration.getString("couchbase.pass").getOrElse(""),
             timeout: Long   = Play.configuration.getLong("couchbase.timeout").getOrElse(0)): Couchbase = {
    new Couchbase(None, host, port, base, bucket, pass, timeout)
  }

  def withCouchbase[T](block: CouchbaseClient => T): T = defaultBucket.withCouchbase(block).get
  def withCouchbase[T](bucketName: String)(block: CouchbaseClient => T): T = bucket(bucketName).withCouchbase(block).get

}
