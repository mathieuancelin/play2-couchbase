Couchbase Plugin for Play framework 2.1
---------------------------------------

How to use it :

in your project/Build.scala file add in the main section :

resolvers += "ancelin" at "https://raw.github.com/mathieuancelin/play2-couchbase/master/repository/snapshots"

and in the appDependencies section :

"org.ancelin.play2.couchbase" %% "play2-couchbase" % "0.1-SNAPSHOT"

create a conf/play.plugins file and add :

400:org.ancelin.play2.couchbase.CouchbasePlugin

add in your conf/application.conf file :

couchbase {
    host="127.0.0.1"
    port="8091"
    base="pools"
    bucket="bucketname"
    pass="bucketpass"
    timeout="0"
    pollfutures=true
    polldelay=50
    execution-context {
        fork-join-executor {
            parallelism-factor = 20.0
            parallelism-max = 200
        }
    }
}

then you will be able to use the couchbase API from your Play controllers

```scala

import play.api.mvc.{Action, Controller}
import play.api.libs.json._
import org.ancelin.play2.couchbase.Couchbase._
import org.ancelin.play2.couchbase.Couchbase
import org.ancelin.play2.couchbase.CouchbaseController
import play.api.Play.current

case class User(name: String, surname: String, email: String)

object UserController extends Controller with CouchbaseController {

  implicit val customExecutionContext = Couchbase.couchbaseExecutor
  implicit val userReader = Json.reads[User]

  def getUser(key: String) = CouchbaseAction { implicit couchbaseclient =>
    get[User](key).map { maybeUser =>
      maybeUser.map(user => Ok(views.html.user(user)).getOrElse(BadRequest(s"Unable to find user with key: $key"))
    }
  }
}

```

