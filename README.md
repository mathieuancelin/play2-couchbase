Couchbase Plugin for Play framework 2.1
---------------------------------------

in your `project/Build.scala` file add dependencies and resolvers like :

```scala

import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "shorturls"
  val appVersion      = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    jdbc, anorm,
    "org.ancelin.play2.couchbase" %% "play2-couchbase" % "0.1-SNAPSHOT"
  )

  val main = play.Project(appName, appVersion, appDependencies).settings(
    resolvers += "ancelin" at "https://raw.github.com/mathieuancelin/play2-couchbase/master/repository/snapshots",
    resolvers += "Spy Repository" at "http://files.couchbase.com/maven2"
  )
}
```

then create a `conf/play.plugins` file and add :

`400:org.ancelin.play2.couchbase.CouchbasePlugin`

add in your `conf/application.conf` file :

```

couchbase {
  execution-context {
    timeout=1000
    pollfutures=true
    polldelay=10
    execution-context {
      fork-join-executor {
        parallelism-factor = 20.0
        parallelism-max = 200
      }
    }
  }
  buckets = [{
    host="127.0.0.1"
    port="8091"
    base="pools"
    bucket="bucketname"
    pass=""
    timeout="0"
  }]
}


```

then you will be able to use the couchbase API from your Play controllers. The following code is asynchronous and uses Play's `Async { ... }`API under the hood. As you will need an execution context for all those async calls, you can use `Couchbase.couchbaseExecutor` based on your `application.conf` file. You can of course use Play default Execution Context (through `import play.api.libs.concurrent.Execution.Implicits._`) or your own.

```scala

import play.api.mvc.{Action, Controller}
import play.api.libs.json._
import org.ancelin.play2.couchbase.Couchbase._
import org.ancelin.play2.couchbase.Couchbase
import org.ancelin.play2.couchbase.CouchbaseBucket
import org.ancelin.play2.couchbase.CouchbaseController
import play.api.Play.current

case class User(name: String, surname: String, email: String)

object UserController extends Controller with CouchbaseController {

  implicit val couchbaseExecutionContext = Couchbase.couchbaseExecutor
  implicit val userReader = Json.reads[User]

  def getUser(key: String) = CouchbaseAction { implicit bucket =>
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
import org.ancelin.play2.couchbase.CouchbaseBucket
import play.api.Play.current

case class User(name: String, surname: String, email: String)

object UserController extends Controller {

  implicit val couchbaseExecutionContext = Couchbase.couchbaseExecutor
  implicit val userReader = Json.reads[User]

  def getUser(key: String) = Action { 
    Async {
      withCouchbase { implicit bucket =>
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
couchbase {

  ...

  buckets = [{
      host=["127.0.0.1", "192.168.0.42"]
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
}

```

then select one of them for each of your operation :

```scala

object UserController extends Controller with CouchbaseController {

  implicit val couchbaseExecutionContext = Couchbase.couchbaseExecutor
  implicit val userReader = Json.reads[User]
  implicit val beerReader = Json.reads[Beer]

  def getUser(key: String) = CouchbaseAction("bucket1") { implicit bucket =>
    get[User](key).map { maybeUser =>
      maybeUser.map(user => Ok(views.html.user(user)).getOrElse(BadRequest(s"Unable to find user with key: $key"))
    }
  }

  def getBeer(key: String) = CouchbaseAction("bucket2") { request => implicit bucket =>
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
import org.ancelin.play2.couchbase.CouchbaseBucket
import play.api.Play.current

case class Beer(id: String, name: String, brewery: String) {
  def save(): Future[OperationStatus] = Beer.save(this)
  def remove(): Future[OperationStatus] = Beer.remove(this)
}

object Beer {

  implicit val beerReader = Json.reads[Beer]
  implicit val beerWriter = Json.writes[Beer]
  implicit val bucket = Couchbase.bucket("bucket2")
  implicit val ec = Couchbase.couchbaseExecutor

  def findById(id: String): Future[Option[Beer]] = {
    get[Beer](id)
  }

  def findAll(): Future[List[Beer]] = {
    find[Beer]("beer", "by_name")(new Query().setIncludeDocs(true).setStale(Stale.FALSE))
  }

  def findByName(name: String): Future[Option[Beer]] = {
    val query = new Query().setIncludeDocs(true).setLimit(1)
          .setRangeStart(ComplexKey.of(name))
          .setRangeEnd(ComplexKey.of(s"$name\uefff").setStale(Stale.FALSE))
    find[Beer]("beer", "by_name")(query).map(_.headOption)
  }

  def save(beer: Beer): Future[OperationStatus] = {
    set[Beer](beer)
  }

  def remove(beer: Beer): Future[OperationStatus] = {
    delete[Beer](beer)
  }
}

```

You can also access Couchbase from your Java application :

```java

package controllers;

import models.ShortURL;
import org.ancelin.play2.java.couchbase.Couchbase;
import org.ancelin.play2.java.couchbase.CouchbaseBucket;
import play.libs.F;
import static play.libs.F.*;
import play.mvc.Controller;
import play.mvc.Result;

public class Application extends Controller {

    public static CouchbaseBucket bucket = Couchbase.bucket("bucket1");

    public static Result  getUser(final String key) {
        return async(
            bucket.get(key, User.class).map(new Function<User, Result>() {
                @Override
                public Result apply(User user) throws Throwable {
                    if (user == null) {
                        return badRequest("Unable to find user with key: " + key);
                    }
                    return ok(views.html.user.render(user));
                }
            })
        );
    }
}

```

and with Java 8

```java

package controllers;

import models.ShortURL;
import org.ancelin.play2.java.couchbase.Couchbase;
import org.ancelin.play2.java.couchbase.CouchbaseBucket;
import play.libs.F;
import static play.libs.F.*;
import play.mvc.Controller;
import play.mvc.Result;

public class Application extends Controller {

    public static CouchbaseBucket bucket = Couchbase.bucket("bucket1");

    public static Result  getUser(final String key) {
        return async(
            bucket.get(key, User.class).map(user -> {
                if (user == null) {
                    return badRequest("Unable to find user with key: " + key);
                }
                return ok(views.html.user.render(user));
            })
        );
    }
}

```

or from model

```java

package models;

import com.couchbase.client.protocol.views.ComplexKey;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import net.spy.memcached.ops.OperationStatus;
import org.ancelin.play2.java.couchbase.Couchbase;
import org.ancelin.play2.java.couchbase.CouchbaseBucket;
import play.libs.F;
import static play.libs.F.*;

import java.util.Collection;

public class ShortURL {

    public String id;
    public String originalUrl;

    public ShortURL() {}

    public ShortURL(String id, String originalUrl) {
        this.id = id;
        this.originalUrl = originalUrl;
    }

    public static CouchbaseBucket bucket = Couchbase.bucket("default");

    public static Promise<ShortURL> findById(String id) {
        return bucket.get(id, ShortURL.class);
    }

    public static Promise<Collection<ShortURL>> findAll() {
        return bucket.find("shorturls", "by_url",
            new Query().setIncludeDocs(true).setStale(Stale.FALSE), ShortURL.class);
    }

    public static Promise<Option<ShortURL>> findByURL(String url) {
        Query query = new Query()
                .setLimit(1)
                .setIncludeDocs(true)
                .setStale(Stale.FALSE)
                .setRangeStart(ComplexKey.of(url))
                .setRangeEnd(ComplexKey.of(url + "\uefff"));
        return bucket.find("shorturls", "by_url", query, ShortURL.class)
                .map(new Function<Collection<ShortURL>, Option<ShortURL>>() {
            @Override
            public Option<ShortURL> apply(Collection<ShortURL> shortURLs) throws Throwable {
                if (shortURLs.isEmpty()) {
                    return Option.None();
                }
                return Option.Some(shortURLs.iterator().next());
            }
        });
    }

    public static Promise<OperationStatus> save(ShortURL url) {
        return bucket.set(url.id, url);
    }

    public static Promise<OperationStatus> remove(ShortURL url) {
        return bucket.delete(url.id);
    }
}

```

and with Java 8

```java

package models;

import com.couchbase.client.protocol.views.ComplexKey;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import net.spy.memcached.ops.OperationStatus;
import org.ancelin.play2.java.couchbase.Couchbase;
import org.ancelin.play2.java.couchbase.CouchbaseBucket;
import play.libs.F;
import static play.libs.F.*;

import java.util.Collection;

public class ShortURL {

    public String id;
    public String originalUrl;

    public ShortURL() {}

    public ShortURL(String id, String originalUrl) {
        this.id = id;
        this.originalUrl = originalUrl;
    }

    public static CouchbaseBucket bucket = Couchbase.bucket("default");

    public static Promise<ShortURL> findById(String id) {
        return bucket.get(id, ShortURL.class);
    }

    public static Promise<Collection<ShortURL>> findAll() {
        return bucket.find("shorturls", "by_url",
            new Query().setIncludeDocs(true).setStale(Stale.FALSE), ShortURL.class);
    }

    public static Promise<Option<ShortURL>> findByURL(String url) {
        Query query = new Query()
                .setLimit(1)
                .setIncludeDocs(true)
                .setStale(Stale.FALSE)
                .setRangeStart(ComplexKey.of(url))
                .setRangeEnd(ComplexKey.of(url + "\uefff"));
        return bucket.find("shorturls", "by_url", query, ShortURL.class)
            .map(shortURLs -> {
                if (shortURLs.isEmpty()) {
                    return Option.None();
                }
                return Option.Some(shortURLs.iterator().next());
            });
    }

    public static Promise<OperationStatus> save(ShortURL url) {
        return bucket.set(url.id, url);
    }

    public static Promise<OperationStatus> remove(ShortURL url) {
        return bucket.delete(url.id);
    }
}

```

CRUD Application
=================

If you want to quickly bootstrap a project with Play2 Couchbase, it's pretty easy, you can use the CRUD utilities.

To do so, first create a model :

```scala

case class ShortURL(id: String, originalUrl: String)

```

then declare an implicit Json Formatter that will be used for Json serialization/deserialization

```scala

import play.api.libs.json.Json

object ShortURL {
  implicit val fmt = Json.format[ShortURL]
}

```

Now you can do two things :
* create a CRUD source
* create a CRUD controller

let's try the CRUD controller

```scala

import org.ancelin.play2.couchbase.crud.CouchbaseCrudSourceController
import models.ShortURL
import models.ShortURLs.fmt
import org.ancelin.play2.couchbase.Couchbase
import play.api.Play.current

object ShortURLController extends CouchbaseCrudSourceController[ShortURL] {
  val bucket = Couchbase.bucket("default")
}

```

and add the following route to your `routes` file

```
->      /urls                       controllers.ShortURLController
```

now you will be able to do :

```

GET     /urls/?doc=docName&view=viewName                    # get all urls according to a view
POST    /urls/                                              # create a url
GET     /urls/{id}                                          # get a url
PUT     /urls/{id}                                          # update url
DELETE  /urls/{id}                                          # delete url
POST    /urls/find/?doc=docName&view=viewName&q=query       # search urls
GET     /urls/stream/?doc=docName&view=viewName&q=query     # search urls as HTTP stream

POST    /urls/batch                                         # create multiple urls
PUT     /urls/batch                                         # update multiple urls
DELETE  /urls/batch                                         # delete multiple urls

```

it is also possible to define default design doc name and view :

```scala

object ShortURLController extends CouchbaseCrudSourceController[ShortURL] {
  val bucket = Couchbase.bucket("default")
  override val defaultViewName = "by_url"
  override val defaultDesignDocname = "shorturls"
}

```

you can also only define a CRUD source containing all needed methods with :

```scala

import org.ancelin.play2.couchbase.crud.CouchbaseCrudSource
import models.ShortURL
import models.ShortURLs.fmt
import org.ancelin.play2.couchbase.Couchbase
import play.api.Play.current

object ShortURLSource extends CouchbaseCrudSource[ShortURL]( Couchbase.bucket("default") ) {}

```

About Java Future to Scala Future conversion
===================================

Couchbase client makes use of Java Future for its async operations. Unfortunally, Java Future API is quite broken and you can't really manage to
transform a Java Future to a Scala Future.

This plugin offers you two way for handling that.

The first one, just block the current thread to get the result of the Java Future (in a custom Execution Context with appropriate configuration).
It's done in the configuration of the plugin through :

```

couchbase {
  execution-context {
    timeout=1000
    pollfutures=false  // here, you tell the plugin to block on Java Futures
    execution-context {      // here it's the custom Execution Context
      fork-join-executor {
        parallelism-factor = 20.0
        parallelism-max = 200
      }
    }
  }

  ...

}

```

It's not a perfect solution, but it works well and eat no memory.

The second way is to use a polling technique to check if Java Future are done and complete a Scala Promise according to it.
This solution comes from : http://stackoverflow.com/questions/11529145/how-do-i-wrap-a-java-util-concurrent-future-in-an-akka-future
It's done in the configuration of the plugin through :

```

couchbase {
  execution-context {
    timeout=1000
    pollfutures=true   // here, you tell the plugin to poll on Java Futures
    polldelay=10       // here, you tell the plugin to poll every 10 ms
    execution-context {
      fork-join-executor {
        parallelism-factor = 20.0
        parallelism-max = 200
      }
    }
  }

  ...

}

```

To achieve this polling, we use an Akka scheduler. Akka scheduler run tasks according to an internal tick and may not run the polling exactly
the way we want, so you can configure the scheduler to be more 'reactive' with :

```

akka {
  scheduler {
    tick-duration = 10ms     // here, tick delay in ms, try to change it according to your couchbase-ec.polldelay
    ticks-per-wheel = 512
  }
}

```

It's also not a perfect solution, because the polling can induce some overhead from a response time point of view.
Let's hope Couchbase guys will introduce some kind of custom completable Java future ;-)

Use Couchbase as Cache implementation
=====================================

in `conf/play.plugins` file and add :
`500:org.ancelin.play2.couchbase.CouchbaseCachePlugin`

then you need to add the following to your configuration

```
couchbase {
  cache {
    bucket="name of the bucket to use"
    enabled=true
  }

  ...

}
```

you can also specify a common namespace for the cache keys with

```
couchbase {
  cache {
    bucket="name of the bucket to use"
    enabled=true
    namespace="play-cache."
  }

  ...

}
```

then you can use your Play Cache the standard way

```scala

Cache.set("item.key", connectedUser)
val maybeUser: Option[User] = Cache.getAs[User]("item.key")
```

Synchonise couchbase design documents
=====================================

You can use CouchbaseEvolutionsPlugin to automaticaly create your design documents at application start from json files.

in `conf/play.plugins` file and add :
`600:org.ancelin.play2.couchbase.CouchbaseEvolutionsPlugin`

then configure the plugin

```
couchbase {

  ...
  evolutions {
    #documents = ...    #optional, default conf/couchbase
    #disabled = ...     #optional, default false
    #use.locks = ...    #optional, default true

    default { #default is the name of your bucket
        apply = true
        synchronise = true
    }
  }

}
```

The plugin will search your design documents in folders named as your buckets in the `couchbase.evolutions.documents` value

```
conf
	couchbase
		default
			tweets.json
			users.json
			...
		...
```

Your json might look like

```json

{
    "views":{
       "withMedia": {
           "map": "function (doc, meta) {\n if(doc.entities.media)\n  emit(dateToArray(doc.created_at))\n}"
       }
    }
}
```

The name of the design document will be the name of the json file. You can specify another one in the json

```json

{
	"name":"my_doc",
    "views":{
       "withMedia": {
           "map": "function (doc, meta) {\n if(doc.entities.media)\n  emit(dateToArray(doc.created_at))\n}"
       }
    }
}
```

**Note : Design documents synchronisation is not yet available**

Git
===================================

If you want to clone this git repo, as we embed snapshot libs (maybe we will move it later), it can be useful to use

`git clone --depth xxx git://github.com/mathieuancelin/play2-couchbase.git`