package models

import akka.actor.{Props, Actor, ActorSystem}
import org.ancelin.play2.couchbase.Couchbase
import scala.concurrent.Future
import akka.pattern.ask
import akka.util.Timeout
import java.util.concurrent.TimeUnit
import play.api.libs.json.Json
import org.ancelin.play2.couchbase.Couchbase._
import com.couchbase.client.protocol.views.{ComplexKey, Stale, Query}
import net.spy.memcached.ops.OperationStatus
import play.api.data.Form
import play.api.data.Forms._
import play.api.Play.current

case class Counter(value: Long)
case class IncrementAndGet()
class IdGenerator extends Actor {

  import models.ShortURLs._

  def receive = {
    case _:IncrementAndGet ⇒ {
      val customSender = sender
      Couchbase.get[Counter](IdGenerator.counterKey)(ShortURLs.client, ShortURLs.counterReader, ShortURLs.ec).map { maybe =>
        maybe.map { value =>
          val newValue = value.copy(value.value + 1L)
          Couchbase.set[Counter](IdGenerator.counterKey, newValue)(ShortURLs.client, ShortURLs.counterWriter, ShortURLs.ec).map { status =>
            customSender.tell(newValue.value, self)
          }
        }.getOrElse {
          Couchbase.set[Counter](IdGenerator.counterKey, Counter(1L))(ShortURLs.client, ShortURLs.counterWriter, ShortURLs.ec).map { status =>
            customSender.tell(1L, self)
          }
        }
      }
    }
  }
}

object IdGenerator {
  implicit val system = ActorSystem("AgentSystem")
  implicit val timeout = Timeout(2, TimeUnit.SECONDS)
  val generator = system.actorOf(Props[IdGenerator], name = "generator")
  val counterKey = "urlidgenerator"
  def nextId(): Future[Long] = {
    (generator ? IncrementAndGet()).mapTo[Long]
  }
}

case class ShortURL(id: String, originalUrl: String, t: String = "shorturl")

object ShortURLs {
  implicit val urlReader = Json.reads[ShortURL]
  implicit val urlWriter = Json.writes[ShortURL]
  implicit val counterReader = Json.reads[Counter]
  implicit val counterWriter = Json.writes[Counter]
  implicit val ec = Couchbase.couchbaseExecutor
  implicit val client = Couchbase.bucket("default").client.get

  val urlForm = Form( "url" -> nonEmptyText )

  def findById(id: String): Future[Option[ShortURL]] = {
    get[ShortURL](id)
  }

  def findAll(): Future[List[ShortURL]] = {
    find[ShortURL]("shorturls", "by_url")( new Query().setIncludeDocs(true).setStale(Stale.FALSE) )
  }

  def findByURL(url: String): Future[Option[ShortURL]] = {
    val query = new Query()
      .setLimit(1)
      .setIncludeDocs(true)
      .setStale(Stale.FALSE)
      .setRangeStart(ComplexKey.of(url))
      .setRangeEnd(ComplexKey.of(s"$url\uefff"))
    find[ShortURL]("shorturls", "by_url")(query).map(_.headOption)
  }

  def save(url: ShortURL): Future[OperationStatus] = {
    set[ShortURL]( url )
  }

  def remove(id: String): Future[OperationStatus] = {
    delete(id)
  }

  def remove(url: ShortURL): Future[OperationStatus] = {
    delete(url)
  }
}