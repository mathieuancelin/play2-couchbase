Couchbase Plugin for Play framework 2.1
---------------------------------------

project/Build.scala :

resolvers += "ancelin" at "https://raw.github.com/mathieuancelin/play2-couchbase/master/repository/snpashots"
libraryDependencies += "org.ancelin.play2.couchbase" %% "play2-couchbase" % "0.1-SNAPSHOT"

conf/play.plugins :
400:org.ancelin.play2.couchbase.CouchbasePlugin

