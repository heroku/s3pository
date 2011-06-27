name := "s3pository"

version := "1.0-SNAPSHOT"

scalaVersion := "2.8.1"

resolvers += "twitter.com" at "http://maven.twttr.com"

mainClass := Some("com.heroku.maven.s3pository.Server")

libraryDependencies ++= Seq(
	"com.twitter" % "finagle-core" % "1.6.1" withSources(),
	"org.scalatest" % "scalatest_2.8.1" % "1.5" withSources() ,
	"org.jboss.netty" % "netty" % "3.2.3.Final" withSources(),
        "joda-time" % "joda-time" % "1.6.2" withSources()
)



