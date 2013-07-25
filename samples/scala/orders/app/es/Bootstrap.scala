package es

import akka.actor.{ActorRef, Props, ActorSystem}
import org.ancelin.play2.couchbase.Couchbase
import org.ancelin.play2.couchbase.store.{EventStored, CouchbaseEventSourcing}
import models.Formatters
import play.api.Play.current
import play.api.{Mode, Play}

object Bootstrap {

  val system: ActorSystem = ActorSystem("es-system")
  val bucket = Couchbase.bucket("es")

  implicit val ec = Couchbase.couchbaseExecutor

  val couchbaseEventSourcing = CouchbaseEventSourcing( system, bucket)
    .registerEventFormatter(Formatters.CreditCardValidatedFormatter)
    .registerEventFormatter(Formatters.CreditCardValidationRequestedFormatter)
    .registerEventFormatter(Formatters.OrderAcceptedFormatter)
    .registerEventFormatter(Formatters.OrderFormatter)
    .registerEventFormatter(Formatters.OrderSubmittedFormatter)
    .registerSnapshotFormatter(Formatters.StateFormatter)

  var processor = couchbaseEventSourcing.processorOf(Props(new OrderProcessor with EventStored))
  var validator = system.actorOf(Props(new CreditCardValidator(processor)))
  var ordersHandler = system.actorOf(Props(new OrdersHandler))

  def bootstrap() = {
    couchbaseEventSourcing.replayAll()
  }

  def shutdown() = {
    system.shutdown()
  }
}
