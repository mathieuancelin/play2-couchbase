package models

import play.api.libs.json.Json

case class User(name: String, age: Int)

object User {

  implicit val userFmt = Json.format[User]

}