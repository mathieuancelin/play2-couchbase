package controllers

import play.api.mvc._
import models.{Order, OrderSubmitted}
import org.ancelin.play2.couchbase.store.Message
import es.{Broadcaster, Bootstrap}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import es.Bootstrap.ec
import play.api.libs.iteratee.{Enumeratee, Concurrent}
import play.api.libs.json.{Json, JsObject}
import play.api.data._
import play.api.data.Forms._

object Application extends Controller {

  implicit val timeout = Timeout(5 seconds)
  
  def index = Action {
    Ok(views.html.index())
  }

  def sse = Action {
    Ok.feed(Broadcaster.enumerator.through(Enumeratee.map { jso => s"data: ${Json.stringify(jso)}\n\n"})).as("text/event-stream")
  }

  val creditCardNumberForm = Form(
    "creditCardNumber" -> text
  )

  def order() = Action { implicit request =>
    creditCardNumberForm.bindFromRequest().fold(
      errors => BadRequest("You have to provide a credit card number"),
      creditCardNumber => {
        val message = Message.create(
          OrderSubmitted(
            Order(details = "jelly beans", creditCardNumber = creditCardNumber)
          )
        )
        Async {
          (Bootstrap.processor ? message).map(_ => Ok("Done !!!"))
        }
      }
    )
  }
}