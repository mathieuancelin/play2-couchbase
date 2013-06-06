import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "play2-couchbase"
  val appVersion      = "0.1-SNAPSHOT"
  val appScalaVersion = "2.10.0"
  val appScalaBinaryVersion = "2.10"
  val appScalaCrossVersions = Seq("2.10.0")

  val local: Project.Initialize[Option[sbt.Resolver]] = version { (version: String) =>
    val localPublishRepo = "./repository"
    if(version.trim.endsWith("SNAPSHOT"))
      Some(Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
    else Some(Resolver.file("releases", new File(localPublishRepo + "/releases")))
  }

  lazy val baseSettings = Defaults.defaultSettings ++ Seq(
    scalaVersion := appScalaVersion,
    scalaBinaryVersion := appScalaBinaryVersion,
    crossScalaVersions := appScalaCrossVersions,
    parallelExecution in Test := false
  )

  lazy val root = Project("root", base = file("."))
    .settings(baseSettings: _*)
    .settings(
      publishLocal := {},
      publish := {}
    ).aggregate(plugin, scalaDummySample, scalaShortUrlsSample)//, javaShortUrlsSample)

  lazy val plugin = Project(appName, base = file("plugin"))
    .settings(baseSettings: _*)
    .settings(
      resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      resolvers += "Spy Repository" at "http://files.couchbase.com/maven2",
      libraryDependencies += "couchbase" % "couchbase-client" % "1.1.6",
      libraryDependencies += "play" %% "play" % "2.1.0" % "provided",
      organization := "org.ancelin.play2.couchbase",
      version := appVersion,
      publishTo <<= local,
      publishMavenStyle := true,
      publishArtifact in Test := false,
      pomIncludeRepository := { _ => false }
    )

    lazy val scalaDummySample = play.Project(
      "scala-dummy-sample",
      path = file("samples/scala/dummy")
    ).settings(
      scalaVersion := appScalaVersion,
      scalaBinaryVersion := appScalaBinaryVersion,
      crossScalaVersions := appScalaCrossVersions,
      crossVersion := CrossVersion.full,
      parallelExecution in Test := false,
      publishLocal := {},
      publish := {}
    ).dependsOn(plugin)

    lazy val scalaShortUrlsSample = play.Project(
      "scala-shorturls-sample",
      path = file("samples/scala/shorturls")
    ).settings(
      scalaVersion := appScalaVersion,
      scalaBinaryVersion := appScalaBinaryVersion,
      crossScalaVersions := appScalaCrossVersions,
      crossVersion := CrossVersion.full,
      parallelExecution in Test := false,
      publishLocal := {},
      publish := {}
    ).dependsOn(plugin)

    lazy val javaShortUrlsSample = play.Project(
      "java-shorturls-sample",
      path = file("samples/java/shorturls")
    ).settings(
      scalaVersion := appScalaVersion,
      scalaBinaryVersion := appScalaBinaryVersion,
      crossScalaVersions := appScalaCrossVersions,
      crossVersion := CrossVersion.full,
      parallelExecution in Test := false,
      publishLocal := {},
      publish := {},
      libraryDependencies += "play" %% "play-java" % "2.1.0" % "provided"
    ).dependsOn(plugin)

}
