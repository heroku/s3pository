import com.typesafe.startscript.StartScriptPlugin

seq(StartScriptPlugin.startScriptForClassesSettings: _*)

name := "s3pository"

version := "1.1"

scalaVersion := "2.9.2"

resolvers ++= Seq("twitter.com" at "http://maven.twttr.com", "newrelic" at "http://download.newrelic.com", "heroku-finagle" at "http://s3.amazonaws.com/heroku-finagle/release")

libraryDependencies ++= Seq(
    "com.twitter" %% "finagle-core" % "6.2.1",
    "com.twitter" %% "finagle-http" % "6.2.1",
    "com.heroku" %% "finagle-aws" % "6.2.1",
    "com.twitter" % "util-logging" % "6.2.4",
    "com.twitter" % "util-collection" % "6.2.4",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test",
    "joda-time" % "joda-time" % "1.6.2",
    "newrelic.java-agent" % "newrelic-api" %  "2.0.4",
    "io.blitz" % "blitz-api-client" %  "0.2.5"
)



