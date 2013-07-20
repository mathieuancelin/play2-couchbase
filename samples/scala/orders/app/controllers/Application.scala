package controllers

import play.api.mvc._
import models.{Order, OrderSubmitted}
import org.ancelin.play2.couchbase.store.Message
import es.Bootstrap
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import es.Bootstrap.ec

object Application extends Controller {

  implicit val timeout = Timeout(5 seconds)
  
  def index = Action {
    Ok(views.html.index())
  }

  def order() = Action {
    val message = Message.create(
      OrderSubmitted(
        Order(details = "jelly beans", creditCardNumber = "1234-5678-1234-5678")
      )
    )
    Async {
      (Bootstrap.processor ? message).map { stuff =>
          println(stuff)
      }.map(_ => Ok("Done !!!"))
    }
  }
}