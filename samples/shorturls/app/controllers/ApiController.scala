package controllers

import play.api._
import play.api.mvc._
import models.{IdGenerator, ShortURLs, ShortURL}
import play.api.libs.json.Json
import models.ShortURLs._
import scala.concurrent.Future
import play.api.Play.current


object ApiController extends Controller {

  def getUrl(id: String) = Action {
    Async {
      ShortURLs.findById(id).map { maybe =>
        maybe.map( url => Ok( Json.toJson(url)) ).getOrElse(NotFound)
      }
    }
  }

  def getAllUrls() =  Action {
    Async {
      ShortURLs.findAll().map(urls => Ok( Json.toJson( urls ) ) )
    }
  }

  def createUrl() = Action { implicit request =>
    Async {
      ShortURLs.urlForm.bindFromRequest.fold(
        errors => Future(BadRequest(
          Json.obj(
            "status" -> "error",
            "error" -> true,
            "created" -> false,
            "message" -> "You need to pass a non empty url value")
          )
        ),
        url => {
          IdGenerator.nextId().flatMap { id =>
            val shortUrl = ShortURL(s"$id", url)
            ShortURLs.save(shortUrl).map { status =>
              status.isSuccess match {
                case true => Ok(
                  Json.obj(
                    "status" -> "created",
                    "error" -> false,
                    "created" -> true,
                    "message" -> status.getMessage,
                    "url" -> shortUrl
                  )
                )
                case false => BadRequest(
                  Json.obj(
                    "status" -> "error",
                    "error" -> true,
                    "created" -> false,
                    "message" -> status.getMessage
                  )
                )
              }
            }
          }
        }
      )
    }
  }

  def delete(id: String) = Action {
    Async {
      ShortURLs.remove(id).map { status =>
        status.isSuccess match {
          case true => Ok(
            Json.obj(
              "status" -> "deleted",
              "error" -> false,
              "deleted" -> true,
              "message" -> status.getMessage
            )
          )
          case false => BadRequest(
            Json.obj(
              "status" -> "error",
              "error" -> true,
              "deleted" -> false,
              "message" -> status.getMessage
            )
          )
        }
      }
    }
  }
}
