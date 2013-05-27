Couchbase Plugin for Play framework 2.1
---------------------------------------

in your `project/Build.scala` file add in the main section :

`resolvers += "ancelin" at "https://raw.github.com/mathieuancelin/play2-couchbase/master/repository/snapshots"`

and in the appDependencies section :

`"org.ancelin.play2.couchbase" %% "play2-couchbase" % "0.1-SNAPSHOT"`

create a `conf/play.plugins` file and add :

`400:org.ancelin.play2.couchbase.CouchbasePlugin`

add in your `conf/application.conf` file :

```

couchbase-ec {
  timeout=1000
  pollfutures=true
  polldelay=50
  execution-context {
    fork-join-executor {
      parallelism-factor = 20.0
      parallelism-max = 200
    }
  }
}

couchbase = [{
    host="127.0.0.1"
    port="8091"
    base="pools"
    bucket="bucketname"
    pass=""
    timeout="0"
}]

```

then you will be able to use the couchbase API from your Play controllers. The following code is asynchronous and uses Play's `Async { ... }`API under the hood. As you will need an execution context for all those async calls, you can use `Couchbase.couchbaseExecutor` based on your `application.conf` file. You can of course use Play default Execution Context (through `import play.api.libs.concurrent.Execution.Implicits._`) or your own.

```scala

import play.api.mvc.{Action, Controller}
import play.api.libs.json._
import org.ancelin.play2.couchbase.Couchbase._
import org.ancelin.play2.couchbase.Couchbase
import org.ancelin.play2.couchbase.CouchbaseController
import play.api.Play.current

case class User(name: String, surname: String, email: String)

object UserController extends Controller with CouchbaseController {

  implicit val couchbaseExecutionContext = Couchbase.couchbaseExecutor
  implicit val userReader = Json.reads[User]

  def getUser(key: String) = CouchbaseAction { implicit couchbaseclient =>
    get[User](key).map { maybeUser =>
      maybeUser.map(user => Ok(views.html.user(user)).getOrElse(BadRequest(s"Unable to find user with key: $key"))
    }
  }
}

```

this code is a shortcut for 

```scala

import play.api.mvc.{Action, Controller}
import play.api.libs.json._
import org.ancelin.play2.couchbase.Couchbase._
import org.ancelin.play2.couchbase.Couchbase
import play.api.Play.current

case class User(name: String, surname: String, email: String)

object UserController extends Controller {

  implicit val couchbaseExecutionContext = Couchbase.couchbaseExecutor
  implicit val userReader = Json.reads[User]

  def getUser(key: String) = Action { 
    Async {
      withCouchbase { implicit couchbaseclient =>
        get[User](key).map { maybeUser =>
          maybeUser.map(user => Ok(views.html.user(user)).getOrElse(BadRequest(s"Unable to find user with key: $key"))
        }
      }
    }
  }
}

```

You can of course connect many buckets with :

```

couchbase = [{
    host="127.0.0.1"
    port="8091"
    base="pools"
    bucket="bucket1"
    pass=""
    timeout="0"
}, {
   host="127.0.0.1"
   port="8091"
   base="pools"
   bucket="bucket2"
   pass=""
   timeout="0"
}, {
   host="192.168.0.42"
   port="8091"
   base="pools"
   bucket="bucket3"
   pass=""
   timeout="0"
}]

```

then select one of them for each of your operation :

```scala

object UserController extends Controller with CouchbaseController {

  implicit val couchbaseExecutionContext = Couchbase.couchbaseExecutor
  implicit val userReader = Json.reads[User]
  implicit val beerReader = Json.reads[Beer]

  def getUser(key: String) = CouchbaseAction("bucket1") { implicit couchbaseclient =>
    get[User](key).map { maybeUser =>
      maybeUser.map(user => Ok(views.html.user(user)).getOrElse(BadRequest(s"Unable to find user with key: $key"))
    }
  }

  def getBeer(key: String) = CouchbaseAction("bucket2") { implicit couchbaseclient =>
    get[Beer](key).map { maybeBeer =>
      maybeBeer.map(beer => Ok(views.html.beer(beer)).getOrElse(BadRequest(s"Unable to find beer with key: $key"))
    }
  }
}

```

or from inside a model :

```scala

import play.api.libs.json._
import org.ancelin.play2.couchbase.Couchbase._
import org.ancelin.play2.couchbase.Couchbase
import play.api.Play.current

case class Beer(id: String, name: String, brewery: String) {
  def save(): Future[OperationStatus] = Beer.save(this)
  def remove(): Future[OperationStatus] = Beer.remove(this)
}

object Beer {

  implicit val beerReader = Json.reads[Beer]
  implicit val beerWriter = Json.writes[Beer]
  implicit val ec = Couchbase.couchbaseExecutor

  // can be declared as implicit to avoid 'bucket.withCouchbase { ... }' usage
  val bucket = Couchbase.bucket("bucket2")

  val byNameFinder = find[Beer]("beer", "by_name")

  def findById(id: String): Future[Option[Beer]] = {
    // implicit syntax
    bucket.withCouchbase { implicit client =>
      get[Beer](id)
    }
  }

  def findByName(name: String): Future[Option[Beer]] = {
    // verbose syntax
    val query = new Query().setIncludeDocs(true).setLimit(1)
          .setRangeStart(ComplexKey.of(name))
          .setRangeEnd(ComplexKey.of(s"$name\uefff"))
    byNameFinder(query)(bucket.client, beerReader, ec).map(_.headOption)
  }

  def save(beer: Beer): Future[OperationStatus] = {
    // verbose syntax
    set[Beer](beer)(bucket.client, beerWriter, ec)
  }

  def remove(beer: Beer): Future[OperationStatus] = {
    // verbose syntax
    delete[Beer](beer)(bucket.client, ec)
  }
}

```


If you want to clone this git repo, as we embed snapshot libs (maybe we will move it later), it can be useful to use

`git clone --depth xxx git://github.com/mathieuancelin/play2-couchbase.git`