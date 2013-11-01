<a href="http://www.reactivemanifesto.org/"> <img style="border: 0; position: fixed; left: 0; top:0; z-index: 9000" src="http://www.reactivemanifesto.org/images/ribbons/we-are-reactive-black-left.png"> </a>
Couchbase Plugin for Play framework 2.2
=======================================

Contents
--------

- [Basic Usage](#basic-usage)
    - [Project configuration](#project-configuration)
    - [Usage from a controller](#standard-usage-from-a-controller)
    - [Usage from a model](#standard-usage-from-a-model)
- [How to build CRUD Application](#crud-application)
- [Capped bucket & tailable queries](#)
- [Use Couchbase as Play Cache](#use-couchbase-as-cache-implementation)
- [Synchronise Design Documents (evolutions)](#synchonize-couchbase-design-documents)
- [Insert data at startup (fixtures)](#automatically-insert-documents-at-startup)
- [Couchbase event store](#couchbase-event-store)
- [Couchbase configuration cheatsheet](#couchbase-configuration-cheatsheet)
- [Git issues](#git)
- [Using Play Framework 2.1](#using-play-21)

Current version
============

* current dev version for Play framework 2.2 is 0.6-SNAPSHOT
  * https://raw.github.com/mathieuancelin/play2-couchbase/master/repository/snapshots
* current version for Play framework 2.2 is 0.5
  * https://raw.github.com/mathieuancelin/play2-couchbase/master/repository/releases
* current version for Play framework 2.1 is 0.4
  * https://raw.github.com/mathieuancelin/play2-couchbase/master/repository/releases

Starter Kits
=============

You can quickly bootstrap a project with the Java and Scala starter kits :

* https://github.com/mathieuancelin/play2-couchbase/raw/master/repository/bin/couchbase-scala-starter.zip
* https://github.com/mathieuancelin/play2-couchbase/raw/master/repository/bin/couchbase-java-starter.zip
* https://github.com/mathieuancelin/play2-couchbase/raw/master/repository/bin/couchbase-crud-starter.zip

Just download the zip file, unzip it, change the app name/version in the `build.sbt` file and you're ready to go.

Basic Usage
============

Project configuration
---------------------

in your `build.sbt` file add dependencies and resolvers like :

```scala

name := "shorturls"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  cache,
  "org.ancelin.play2.couchbase" %% "play2-couchbase" % "0.6-SNAPSHOT"
)

resolvers += "ancelin" at "https://raw.github.com/mathieuancelin/play2-couchbase/master/repository/snapshots"

resolvers += "Spy Repository" at "http://files.couchbase.com/maven2"

play.Project.playScalaSettings
```

or if you use the good old `project\Build.scala` file :


```scala

import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "shorturls"
  val appVersion      = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    cache,
    "org.ancelin.play2.couchbase" %% "play2-couchbase" % "0.6-SNAPSHOT"
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
    user=""
    pass=""
    timeout="0"
  }]
}

```

Standard usage from a controller
---------------------

You will then be able to use the couchbase API from your Play controllers. The following code is asynchronous and uses Play's `Action.async { ... }`API under the hood. As you will need an execution context for all those async calls, you can use `Couchbase.couchbaseExecutor` based on your `application.conf` file. You can of course use Play default Execution Context (through `import play.api.libs.concurrent.Execution.Implicits._`) or your own.

```scala

import play.api.mvc.{Action, Controller}
import play.api.libs.json._
import org.ancelin.play2.couchbase.Couchbase
import org.ancelin.play2.couchbase.CouchbaseBucket
import org.ancelin.play2.couchbase.CouchbaseController
import play.api.Play.current

case class User(name: String, surname: String, email: String)

object UserController extends Controller with CouchbaseController {

  implicit val couchbaseExecutionContext = Couchbase.couchbaseExecutor
  implicit val userReader = Json.reads[User]

  def getUser(key: String) = CouchbaseAction { bucket =>
    bucket.get[User](key).map { maybeUser =>
      maybeUser.map(user => Ok(views.html.user(user)).getOrElse(BadRequest(s"Unable to find user with key: $key"))
    }
  }
}

```

this code is a shortcut for 

```scala

import play.api.mvc.{Action, Controller}
import play.api.libs.json._
import org.ancelin.play2.couchbase.Couchbase
import org.ancelin.play2.couchbase.CouchbaseBucket
import play.api.Play.current

case class User(name: String, surname: String, email: String)

object UserController extends Controller {

  implicit val couchbaseExecutionContext = Couchbase.couchbaseExecutor
  implicit val userReader = Json.reads[User]

  def getUser(key: String) = Action.async {
    val bucket = Couchbase.defaultBucket
    bucket.get[User](key).map { maybeUser =>
      maybeUser.map(user => Ok(views.html.user(user)).getOrElse(BadRequest(s"Unable to find user with key: $key"))
    }
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

You can of course connect many buckets with :

```
couchbase {

  ...

  buckets = [{
      host=["127.0.0.1", "192.168.0.42"]
      port="8091"
      base="pools"
      bucket="bucket1"
      user=""
      pass=""
      timeout="0"
  }, {
     host="127.0.0.1"
     port="8091"
     base="pools"
     bucket="bucket2"
     user=""
     pass=""
     timeout="0"
  }, {
     host="192.168.0.42"
     port="8091"
     base="pools"
     bucket="bucket3"
     user=""
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

  def getUser(key: String) = CouchbaseAction("bucket1") { bucket =>
    bucket.get[User](key).map { maybeUser =>
      maybeUser.map(user => Ok(views.html.user(user)).getOrElse(BadRequest(s"Unable to find user with key: $key"))
    }
  }

  def getBeer(key: String) = CouchbaseAction("bucket2") { request => bucket =>
    bucket.get[Beer](key).map { maybeBeer =>
      maybeBeer.map(beer => Ok(views.html.beer(beer)).getOrElse(BadRequest(s"Unable to find beer with key: $key"))
    }
  }
}

```

Standard usage from a model
---------------------

```scala

import play.api.libs.json._
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
  implicit val ec = Couchbase.couchbaseExecutor

  def bucket = Couchbase.bucket("bucket2")

  def findById(id: String): Future[Option[Beer]] = {
    bucket.get[Beer](id)
  }

  def findAll(): Future[List[Beer]] = {
    bucket.find[Beer]("beer", "by_name")(new Query().setIncludeDocs(true).setStale(Stale.FALSE))
  }

  def findByName(name: String): Future[Option[Beer]] = {
    val query = new Query().setIncludeDocs(true).setLimit(1)
          .setRangeStart(ComplexKey.of(name))
          .setRangeEnd(ComplexKey.of(s"$name\uefff").setStale(Stale.FALSE))
    bucket.find[Beer]("beer", "by_name")(query).map(_.headOption)
  }

  def save(beer: Beer): Future[OperationStatus] = {
    bucket.set[Beer](beer)
  }

  def remove(beer: Beer): Future[OperationStatus] = {
    bucket.delete[Beer](beer)
  }
}

```

or from a java model

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
  def bucket = Couchbase.bucket("default")
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
PUT     /urls/{id}/partial                                  # partially update url
DELETE  /urls/{id}                                          # delete url
POST    /urls/find/?doc=docName&view=viewName&q=query       # search urls
GET     /urls/stream/?doc=docName&view=viewName&q=query     # search urls as HTTP stream

POST    /urls/batch                                         # create multiple urls
PUT     /urls/batch                                         # update multiple urls
DELETE  /urls/batch                                         # delete multiple urls

```

it is also possible to define default design doc name, view and id key :

```scala

object ShortURLController extends CouchbaseCrudSourceController[ShortURL] {
  def bucket = Couchbase.bucket("default")
  override def defaultViewName = "by_url"
  override def defaultDesignDocname = "shorturls"
  override def idKey = "__myAwesomeIdKey"
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

You can also access this feature from Java

```java

public class ShortURLController extends CrudSourceController<ShortURL> {

    private final CrudSource<ShortURL> source = new CrudSource<ShortURL>( Couchbase.bucket("default"), ShortURL.class );
    //private final CrudSource<ShortURL> source = new CrudSource<ShortURL>( Couchbase.bucket("default"), "__myAwesomeIdKey", ShortURL.class );

    @Override
    public CrudSource<ShortURL> getSource() {
        return source;
    }

    @Override
    public String defaultDesignDocname() {
        return "shorturls";
    }

    @Override
    public String defaultViewName() {
        return "by_url";
    }
}

```

You will need a controller instanciator for that :

```java

import controllers.ShortURLController;
import play.GlobalSettings;

public class Global extends GlobalSettings {
    // You can really do something more clever here ;-)
    public <A> A getControllerInstance(java.lang.Class<A> aClass) throws java.lang.Exception {
        if (aClass.equals(ShortURLController.class)) {
            return (A) new ShortURLController();
        }
        throw new RuntimeException("Cannot instanciate " + aClass.getName());
    }
}

```

and also to define routes (yeah I know it's boring, but it's a work in progress right now)

```

GET     /urls/                      @controllers.ShortURLController.find()
POST    /urls/                      @controllers.ShortURLController.insert()
POST    /urls/find                  @controllers.ShortURLController.find()
POST    /urls/batch                 @controllers.ShortURLController.batchInsert()
PUT     /urls/batch                 @controllers.ShortURLController.batchUpdate()
DELETE  /urls/batch                 @controllers.ShortURLController.batchDelete()
PUT     /urls/:id/partial           @controllers.ShortURLController.updatePartial(id)
GET     /urls/:id                   @controllers.ShortURLController.get(id)
DELETE  /urls/:id                   @controllers.ShortURLController.delete(id)
PUT     /urls/:id                   @controllers.ShortURLController.update(id)

```

Note : **You can also use the awesome play-autosource project (https://github.com/mandubian/play-autosource) that comes with Couchbase support**


Capped buckets and tailable queries
====================================

Play2-couchbase provides a way to simulate capped buckets (http://docs.mongodb.org/manual/core/capped-collections/). 
You can see a capped bucket as a circular buffer. Once the buffer is full, the oldest entry is removed from the bucket.

Here, the bucket isn't really capped at couchbase level. It is capped at play2-couchbase level.
You can use a bucket as a capped bucket using :

```scala

def bucket = Couchbase.cappedBucket("default", 100) // here I use the default bucket as a capped bucket of size 100
```

of course, only data inserted with this `CappedBucket` object are considered for capped bucket features.

```scala

val john = Json.obj("name" -> "John", "fname" -> "Doe")

for (i <- 0 to 200) {
    bucket.insert(UUID.randomUUID().toString, john)
}
// still 100 people in the bucket (and possibly other data inserted with standard API)
```

When a json object is inserted, a timestamp is add to the object and this timestamp will be used to manage all the capped bucket features.

The nice part with capped buckets is the `tail` function. It's like using a `tail -f`command on the datas of the capped bucket

```scala

def tailf = Action.async {
    
    val enumerator1 = bucket.tail[JsValue]()
    val enumerator2 = bucket.tail[JsValue](1265457L) // start to read data from 1265457L timestamp
    val enumerator3 = bucket.tail[JsValue](1265457L, 200, TimeUnit.MILLISECONDS) // update every 200 milliseconds

    enumerator1.map( Ok.chunked( _ ) )
}
```

Use Couchbase as Cache implementation
=====================================

in `conf/play.plugins` file and add :
`500:org.ancelin.play2.couchbase.plugins.CouchbaseCachePlugin`

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

Synchonize Couchbase design documents
=====================================

You can use CouchbaseEvolutionsPlugin to automaticaly create your design documents at application start from json files.

in `conf/play.plugins` file and add :
`600:org.ancelin.play2.couchbase.plugins.CouchbaseEvolutionsPlugin`

then configure the plugin

```
couchbase {

  ...
  evolutions {
    #documents = ...    #optional, default conf/views
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
    views
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


Automatically insert documents at startup
=====================================

You can use CouchbaseFixturesPlugin to automaticaly insert data into buckets.

in `conf/play.plugins` file and add :
`700:org.ancelin.play2.couchbase.plugins.CouchbaseFixturesPlugin`

then configure the plugin

```
couchbase {
  ...
  fixtures {
    #documents = ...    #optional, default conf/fixtures
    #disabled = ...     #optional, default false

    default { #default is the name of your bucket
      insert = true
      key = "_id" # the json member to extract key for insertion
    }
  }
}
```

The plugin will search data in folders named as your buckets in the `couchbase.fixtures.documents` value

```
conf
    fixtures
        default
            users.json
```

Your json **MUST BE** an **ARRAY** structure containing your JSON documents (even if there is only one doc inside) and might look like :

```json

[
    {
        "_id": "1",
        "name": "John Doe",
        "email": "john.doe@gmail.com",
        "password": "password"
    },
    {
        "_id": "2",
        "name": "Jane Doe",
        "email": "jane.doe@gmail.com",
        "password": "password"
    }
]
```

**Note : Your documents will be overwritten at each startup. Removed json files won't be removed from your buckets.**

Couchbase Event Store
===================================

The Couchbase plugin provide a very simple way to store application events. You can use it that way :

```scala

case class CartCreated(customerId: Long, message: String)

object CartCreated {
  val cartCreatedFormat = Json.format[CartCreated]
}

object EventSourcingBoostrap {

  lazy val couchbaseES = CouchbaseEventSourcing( ActorSystem("couchbase-es-1"), Couchbase.bucket("es") )
    .registerFormatter(CartCreated.cartCreatedFormat)

  def bootstrap() = {
    couchbaseES.replayAll()
  }
}

object Cart {
  val cartProcessor = EventSourcingBoostrap.couchbaseES
                        .processorOf(Props(new CartProcessor with EventStored))
  def createCartForUser(user: User) {
    cartProcessor ! Message.create( CartCreated(user.id, "Useful message") )
  }
}

class CartProcessor extends Actor {
  // Application state is here !!!
  var numberOfCreatedCart = 0
  def receive = {
    case msg: CartCreated => {
      numberOfCreatedCart = numberOfCreatedCart + 1
      println( s"[CartProcessor] live carts ${counter} - Last message (${msg.message})" )
    }
    case _ =>
  }
}

val user1 = User( ... )
val user2 = User( ... )
val user3 = User( ... )

EventSourcingBoostrap.bootstrap()
// prints nothing if bucket is empty

Cart.createCartForUser( user1 )
// prints : [CartProcessor] live carts 1 - Last message (Useful message)
Cart.createCartForUser( user2 )
// prints : [CartProcessor] live carts 2 - Last message (Useful message)
Cart.createCartForUser( user3 )
// prints : [CartProcessor] live carts 3 - Last message (Useful message)

EventSourcingBoostrap.bootstrap()
// prints : [CartProcessor] live carts 4 - Last message (Useful message)
// prints : [CartProcessor] live carts 5 - Last message (Useful message)
// prints : [CartProcessor] live carts 6 - Last message (Useful message)

Cart.createCartForUser( user1 )
// prints : [CartProcessor] live carts 7 - Last message (Useful message)
Cart.createCartForUser( user2 )
// prints : [CartProcessor] live carts 8 - Last message (Useful message)
Cart.createCartForUser( user3 )
// prints : [CartProcessor] live carts 9 - Last message (Useful message)

```

Couchbase N1QL search
=======================

N1QL is the Couchbase Query Language. The N1QL Developer Preview 1 (DP1) is a pre-beta release of the language and is available at

http://www.couchbase.com/communities/n1ql

The play2-couchbase plugin offers a very experimental access to N1QL based on the N1QL DP1. As it is experimental, I can not ensure that this feature will not massively change and/or will be continued.

First setup your N1QL query server. Download it and expand it. Then connect it to your Couchbase server.

./cbq-engine -couchbase http://<coucbhase-server-name>:8091/`

Now you have to enable the N1QL plugin and configure it :

in `conf/play.plugins` file and add :
`1000:org.ancelin.play2.couchbase.plugins.CouchbaseN1QLPlugin`

and in you `conf/application.conf` file add :

```

couchbase {
   n1ql {
     host="127.0.0.1"
     port=8093
   }
}

```

And now you can use it from your Play2 application

```scala

package controllers

import play.api.mvc._

import org.ancelin.play2.couchbase.plugins.CouchbaseN1QLPlugin._
import play.api.libs.json.Json
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.iteratee.{Enumerator, Enumeratee}

case class Person(fname: String, age: Int)

object N1QLController extends Controller {

  implicit val personFormat = Json.format[Person]

  def find(age: Int) = Action.async {
    N1QL( s""" SELECT fname, age FROM tutorial WHERE age > ${age} """ )
                                                   .toList[Person].map { persons =>
      Ok(views.html.index(s"Persons older than ${age}", persons))
    }
  }
}

```

or use it the Enumerator way

```scala

package controllers

import play.api.mvc._

import org.ancelin.play2.couchbase.plugins.CouchbaseN1QLPlugin._
import play.api.libs.json.Json
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.iteratee.{Enumerator, Enumeratee}

case class Person(fname: String, age: Int)

object N1QLController extends Controller {

  implicit val personFormat = Json.format[Person]

  def find(age: Int) = Action.async {
    N1QL( s""" SELECT fname, age FROM tutorial WHERE age > ${age} """ )
                                    .enumerate[Person].map { enumerator =>
     Ok.stream(
       (enumerator &>
        Enumeratee.collect[Person] { case p@Person(_, age) if age < 50 => p } ><>
        Enumeratee.map[Person](personFormat.writes)) >>>
        Enumerator.eof
     )
    }
  }
}

```

Couchbase configuration cheatsheet
===================================

Here is the complete plugin configuration with default values

```

couchbase {
  execution-context {               # execution context configuration, optional
    timeout=1000                    # default timeout for futures (ms), optional
    execution-context {             # actual execution context configuration if needed, optional
      fork-join-executor {
        parallelism-factor = 20.0
        parallelism-max = 200
      }
    }
  }
  buckets = [{                      # available bucket. It's an array, so you can define multiple values
    host="127.0.0.1", "127.0.0.1"   # Couchbase hosts, can be multiple comma separated values
    port="8091"
    base="pools"
    bucket="$bucketname"
    pass="$password"
    timeout="0"
  }, {
    host="127.0.0.1", "127.0.0.1"
    port="8091"
    base="pools"
    bucket="$bucketname1"
    pass="$password"
    timeout="0"
  }]
   failfutures=false                 # fail Scala future if OperationStatus is failed, optional
   useplayec=true                    # the plugin can use play.api.libs.concurrent.Execution.Implicits.defaultContext as execution context, optional
   json {                            # JSON related configuration, optional
     validate=true                   # JSON structure validation fail
   }
   driver {                          # couchbase driver related config
     useec=true                      # use couchbase-executioncontext as ExecutorService for couchbase driver, optional
   }
   cache {                           # cache related configuration, optional
     namespace=""                    # key prefix if couchbase used as cache with the cache plugin, optional
     enabled=false                   # enable cache with couchbase, optional
   }
   fixtures {                        # fixtures related configuration, optional
     disabled=false                  # disable fixtures, optional
     documents="conf/fixtures"       # path for fixtures files, optional
     $bucketName {
       insert=true                   # insert fixtures, optional
       key="_id"                     # key name for each fixture object, optional
     }
   }
   evolutions {                      # evolutions related configuration, optional
     disabled=false                  # disable evolutions, optional
     documents="conf/views"          # path for evolution files, optional
     use.locks=true                  # use locks for evolutions, optional
     $bucketName {
       apply=true                    # apply evolution on start
       synchronize=true              # synchronize with existing
     }
   }
   n1ql {                            # N1QL access from API, optionnal
     host="127.0.0.1"                # Host of the N1QL search server
     port=8093                       # Port of the N1QL search server
   }
}

```

Git
===================================

If you want to clone this git repo, as we embed snapshot libs (maybe we will move it later), it can be useful to use

`git clone --depth xxx git://github.com/mathieuancelin/play2-couchbase.git`


Using Play 2.1
===================================

You can use play-couchbase inside a Play 2.1 application, just use the following project description

```scala

import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "shorturls"
  val appVersion      = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    jdbc, anorm,
    "org.ancelin.play2.couchbase" %% "play2-couchbase" % "0.4"
  )

  val main = play.Project(appName, appVersion, appDependencies).settings(
    resolvers += "ancelin" at "https://raw.github.com/mathieuancelin/play2-couchbase/master/repository/releases",
    resolvers += "Spy Repository" at "http://files.couchbase.com/maven2"
  )
}
```
