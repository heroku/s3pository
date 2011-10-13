import com.typesafe.startscript.StartScriptPlugin

seq(StartScriptPlugin.startScriptForClassesSettings: _*)

name := "s3pository"

version := "1.0"

scalaVersion := "2.8.1"

resolvers ++= Seq("twitter.com" at "http://maven.twttr.com", "newrelic" at "http://download.newrelic.com")

libraryDependencies ++= Seq(
	"com.twitter" % "finagle-core" % "1.9.0" withSources(),
	"com.twitter" % "finagle-http" % "1.9.0" withSources(),
	"com.twitter" % "util-logging" % "1.11.4" withSources(),
	"org.scalatest" % "scalatest_2.8.1" % "1.5" withSources() ,
	"org.jboss.netty" % "netty" % "3.2.5.Final" withSources(),
    "joda-time" % "joda-time" % "1.6.2" withSources(),
    "newrelic.java-agent" % "newrelic-api" %  "2.0.4",
    "io.blitz" % "blitz-api-client" %  "0.1.2"
)



