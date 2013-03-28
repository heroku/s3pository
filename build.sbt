import com.typesafe.sbt.SbtStartScript

seq(SbtStartScript.startScriptForClassesSettings:_*)

name := "s3pository"

version := "1.0"

scalaVersion := "2.9.2"

resolvers ++= Seq("twitter.com" at "http://maven.twttr.com", "newrelic" at "http://download.newrelic.com", "heroku-finagle" at "http://s3.amazonaws.com/heroku-finagle/release", "heroku-finagle-snapshot" at "https://raw.github.com/ryanbrainard/repo/master")

libraryDependencies ++= Seq(
	"com.twitter" %% "finagle-core" % "6.2.1" withSources(),
	"com.twitter" %% "finagle-http" % "6.2.1" withSources(),
	"com.heroku" %% "finagle-aws" % "6.2.1-SNAPSHOT",
	"com.twitter" % "util-logging" % "6.2.4" withSources(),
	"com.twitter" % "util-collection" % "6.2.4" withSources(),
    "org.scalatest" %% "scalatest" % "1.9.1" % "test",
    "joda-time" % "joda-time" % "1.6.2" withSources(),
    "newrelic.java-agent" % "newrelic-api" %  "2.0.4",
    "io.blitz" % "blitz-api-client" %  "0.2.5"
)



