package es

import akka.actor.{ActorRef, Actor}
import models._
import models.CreditCardValidated
import models.OrderSubmitted
import models.CreditCardValidationRequested
import scala.concurrent.Future
import org.ancelin.play2.couchbase.store.Message

class OrderProcessor extends Actor {

  var state = State(List.empty[OrderTuple], System.currentTimeMillis())

  def receive = {
    case OrderSubmitted(order) => {
      val id = state.orders.size
      val upd = order.copy(id = id)
      state = state.copy(state.orders :+ OrderTuple(id, upd))
      println("ask for validation")
      println(state)
      Bootstrap.validator.forward(CreditCardValidationRequested(upd))
    }
    case CreditCardValidated(orderId) => {
      state.orders.find(_.id == orderId).foreach { order =>
        val upd = order.order.copy(validated = true)
        state = state.copy(state.orders :+ OrderTuple(orderId, upd))
        println("it's validated, sending to sender and destination")
        println(state)
        sender ! upd
        Bootstrap.destination ! OrderAccepted(upd)
      }
    }
    case _ =>
  }
}

class CreditCardValidator(orderProcessor: ActorRef) extends Actor {
  import Bootstrap.ec
  def receive = {
    case ccvr: CreditCardValidationRequested => {
      val sdr = sender
      val msg = ccvr
      println("Validating ...")
      Future {
        //Thread.sleep(2000)
        val ccv =  CreditCardValidated(msg.order.id)
        println("validated!!")
        orderProcessor.tell(Message.create(ccv), sdr)
      }
    }
    case _ =>
  }
}

class Destination extends Actor {
  def receive = {
    case OrderAccepted(upd) => println("received event %s" format upd)
    case _ =>
  }
}