Couchbase Plugin for Play framework 2.1
---------------------------------------

project/Build.scala :

resolvers += "ancelin" at "http://foo.bar/couchbase"
libraryDependencies += "org.ancelin.play2.couchbase" %% "play2-couchbase" % "0.1"

conf/play.plugins :
400:org.ancelin.play2.couchbase.CouchbasePlugin

