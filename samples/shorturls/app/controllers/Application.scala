package controllers

import play.api._
import play.api.mvc._
import models.ShortURLs
import play.api.libs.json.Json
import play.api.Play.current
import models.ShortURLs._

object Application extends Controller {
  
  def index = Action {
    Ok(views.html.index())
  }

  def all = Action {
    Ok(views.html.all())
  }

  def goTo(id: String) = Action {
    Async {
      ShortURLs.findById(id).map { maybe =>
        maybe.map( url => Redirect(url.originalUrl) ).getOrElse(NotFound)
      }
    }
  }
}