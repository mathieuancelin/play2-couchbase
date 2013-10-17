name := "couchbase-java-starter"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  cache,
  "org.ancelin.play2.couchbase" %% "play2-couchbase" % "0.5-SNAPSHOT"
)

resolvers += "ancelin" at "https://raw.github.com/mathieuancelin/play2-couchbase/master/repository/snapshots"

resolvers += "Spy Repository" at "http://files.couchbase.com/maven2"

play.Project.playJavaSettings
