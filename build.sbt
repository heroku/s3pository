name := "s3pository"

version := "1.0-SNAPSHOT"

scalaVersion := "2.8.1"

resolvers += "twitter.com" at "http://maven.twttr.com"

resolvers += Resolver.file("newrelic", file("."))

mainClass := Some("com.heroku.maven.s3pository.S3rver")

libraryDependencies ++= Seq(
	"com.twitter" % "finagle-core" % "1.9.0" withSources(),
	"com.twitter" % "finagle-http" % "1.9.0" withSources(),
	"com.twitter" % "util-logging" % "1.11.4" withSources(),
	"org.scalatest" % "scalatest_2.8.1" % "1.5" withSources() ,
	"org.jboss.netty" % "netty" % "3.2.5.Final" withSources(),
    "joda-time" % "joda-time" % "1.6.2" withSources(),
    "newrelic" % "newrelic-api" %  "2.0.3",
    "io.blitz" % "blitz-api-client" %  "0.1.2"
)



