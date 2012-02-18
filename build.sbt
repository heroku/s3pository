import com.typesafe.startscript.StartScriptPlugin

seq(StartScriptPlugin.startScriptForClassesSettings: _*)

name := "s3pository"

version := "1.0"

scalaVersion := "2.9.1"

resolvers ++= Seq("twitter.com" at "http://maven.twttr.com", "newrelic" at "http://download.newrelic.com", "heroku-finagle" at "http://s3.amazonaws.com/heroku-finagle/release")

libraryDependencies ++= Seq(
	"com.twitter" % "finagle-core_2.9.1" % "1.11.1" withSources(),
	"com.twitter" % "finagle-http_2.9.1" % "1.11.1" withSources(),
	"com.heroku" % "finagle-aws_2.9.1" % "1.11.1",
	"com.twitter" % "util-logging" % "1.12.13" withSources(),
    "org.scalatest" %% "scalatest" % "1.7.1" % "test",
    "joda-time" % "joda-time" % "1.6.2" withSources(),
    "newrelic.java-agent" % "newrelic-api" %  "2.0.4",
    "io.blitz" % "blitz-api-client" %  "0.1.2"
)



