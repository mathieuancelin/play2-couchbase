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
import org.ancelin.play2.couchbase.CouchbasePlugin

class CouchbaseBucket(val client: Option[CouchbaseClient], val hosts: List[String], val port: String, val base: String, val bucket: String, val pass: String, val timeout: Long) {

  def connect() = {
    val uris = ArrayBuffer(hosts.map { h => URI.create(s"http://$h:$port/$base")}:_*)
    val client = new CouchbaseClient(uris, bucket, pass)
    new CouchbaseBucket(Some(client), hosts, port, base, bucket, pass, timeout)
  }

  def disconnect() = {
    client.map(_.shutdown(timeout, TimeUnit.SECONDS))
    new CouchbaseBucket(None, hosts, port, base, bucket, pass, timeout)
  }

  def withCouchbase[T](block: CouchbaseBucket => T): Option[T] = {
    client.map(_ => block(this))
  }

  def couchbaseClient: CouchbaseClient = {
    client.getOrElse(throw new PlayException(s"Error with bucket ${bucket}", s"Bucket '${bucket}' is not defined or client is not connected"))
  }
}

object Couchbase extends ClientWrapper {

  private val initMessage = "The CouchbasePlugin has not been initialized! Please edit your conf/play.plugins file and add the following line: '400:package org.ancelin.play2.couchbase.CouchbasePlugin' (400 is an arbitrary priority and may be changed to match your needs)."
  private val connectMessage = "The CouchbasePlugin doesn't seems to be connected to a Couchbase server. Maybe an error occured!"
  private val couchbaseActorSystem = ActorSystem("couchbase-plugin-system")

  def defaultBucket(implicit app: Application): CouchbaseBucket = app.plugin[CouchbasePlugin] match {
    case Some(plugin) => plugin.buckets.headOption.getOrElse(throw new PlayException("CouchbasePlugin Error", connectMessage))._2
    case _ => throw new PlayException("CouchbasePlugin Error", initMessage)
  }

  def bucket(bucket: String)(implicit app: Application): CouchbaseBucket = buckets(app).get(bucket).getOrElse(throw new PlayException(s"Error with bucket $bucket", s"Bucket '$bucket' is not defined"))
  def client(bucket: String)(implicit app: Application): CouchbaseClient = buckets(app).get(bucket).flatMap(_.client).getOrElse(throw new PlayException(s"Error with bucket $bucket", s"Bucket '$bucket' is not defined or client is not connected"))

  def buckets(implicit app: Application): Map[String, CouchbaseBucket] =  app.plugin[CouchbasePlugin] match {
    case Some(plugin) => plugin.buckets
    case _ => throw new PlayException("CouchbasePlugin Error", initMessage)
  }


  def couchbaseExecutor(implicit app: Application): ExecutionContext = {
    app.configuration.getObject("couchbase.execution-context.execution-context") match {
      case Some(_) => couchbaseActorSystem.dispatchers.lookup("couchbase.execution-context.execution-context")
      case _ => throw new PlayException("Configuration issue","You have to define a 'couchbase.execution-context.execution-context' object in the application.conf file.")
      // TODO : provide an out of the box executor
    }
  }

  def apply(
             hosts:   List[String] = List(Play.configuration.getString("couchbase.bucket.host").getOrElse("127.0.0.1")),
             port:    String = Play.configuration.getString("couchbase.bucket.port").getOrElse("8091"),
             base:    String = Play.configuration.getString("couchbase.bucket.base").getOrElse("pools"),
             bucket:  String = Play.configuration.getString("couchbase.bucket.bucket").getOrElse("default"),
             pass:    String = Play.configuration.getString("couchbase.bucket.pass").getOrElse(""),
             timeout: Long   = Play.configuration.getLong("couchbase.bucket.timeout").getOrElse(0)): CouchbaseBucket = {
    new CouchbaseBucket(None, hosts, port, base, bucket, pass, timeout)
  }

  def withCouchbase[T](block: CouchbaseBucket => T): T = defaultBucket.withCouchbase(block).get
  def withCouchbase[T](bucketName: String)(block: CouchbaseBucket => T): T = bucket(bucketName).withCouchbase(block).get

}

/*object CouchbaseImplicitConversion {
  implicit def Couchbase2CouchbaseClient(value : CouchbaseBucket): CouchbaseClient = value.client.getOrElse(throw new PlayException(s"Error with bucket ${value.bucket}", s"Bucket '${value.bucket}' is not defined or client is not connected"))
  implicit def Couchbase2ClientWrapper(value : CouchbaseBucket) = {
    new CouchbaseBucket(value.client, value.hosts, value.port, value.base, value.bucket, value.pass, value.timeout) with ClientWrapper
  }
}*/
