package es

import akka.actor.{Props, ActorSystem}
import org.ancelin.play2.couchbase.Couchbase
import org.ancelin.play2.couchbase.store.{EventStored, CouchbaseEventSourcing}
import models.Formatters
import play.api.Play.current

object Bootstrap {

  val system = ActorSystem("es-system")
  val bucket = Couchbase.bucket("es")

  implicit val ec = Couchbase.couchbaseExecutor

  val couchbaseEventSourcing = CouchbaseEventSourcing( system, bucket)
    .registerEventFormatter(Formatters.CreditCardValidatedFormatter)
    .registerEventFormatter(Formatters.CreditCardValidationRequestedFormatter)
    .registerEventFormatter(Formatters.OrderAcceptedFormatter)
    .registerEventFormatter(Formatters.OrderFormatter)
    .registerEventFormatter(Formatters.OrderSubmittedFormatter)
    .registerSnapshotFormatter(Formatters.StateFormatter)
  val processor = couchbaseEventSourcing.processorOf(Props(new OrderProcessor with EventStored))
  val validator = system.actorOf(Props(new CreditCardValidator(processor)))
  val destination = system.actorOf(Props(new Destination))

  def bootstrap() = {
    couchbaseEventSourcing.replayAll()
  }

  def shutdown() = {
    system.shutdown()
  }
}
