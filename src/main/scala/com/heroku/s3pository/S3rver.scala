package com.heroku.s3pository


import com.twitter.conversions.storage._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.Http
import com.twitter.logging.Logger
import com.twitter.logging.config.{ConsoleHandlerConfig, LoggerConfig}

import java.net.InetSocketAddress

import util.Properties
import com.heroku.finagle.aws.S3.{S3Secret, S3Key}


object S3rver {

  val s3prefix = Properties.envOrNone("S3_PREFIX").getOrElse {
    System.out.println("S3_PREFIX env var not defined, exiting")
    System.exit(666)
    "noPrefix"
  }

  /*Wire up the proxied repositories*/
  val proxies = List(/*proxy prefix          source repo host                          source repo path to m2 repo             S3 bucket to store cached content */
    ProxiedRepository("/maven-central", "repo1.maven.org", "/maven2", s3prefix + "-proxy-central"),
    ProxiedRepository("/maven-spring-releases", "maven.springframework.org", "/release", s3prefix + "-proxy-spring-releases").include("/com/springsource").include("/org/springframework").include("/org/aspectj"),
    ProxiedRepository("/maven-spring-milestones", "maven.springframework.org", "/milestone", s3prefix + "-proxy-spring-milestones").include("/com/springsource").include("/org/springframework").include("/org/aspectj"),
    ProxiedRepository("/maven-spring-roo", "spring-roo-repository.springsource.org", "/release", s3prefix + "-proxy-spring-roo").include("/org/springframework/roo"),
    ProxiedRepository("/maven-jboss", "repository.jboss.org", "/nexus/content/repositories/releases", s3prefix + "-proxy-jboss", 443, true).include("/jboss").include("/org/jboss").include("/javax/validation").include("/org/hibernate"),
    ProxiedRepository("/maven-sonatype-oss", "oss.sonatype.org", "/content/repositories/snapshots", s3prefix + "-proxy-sonatype-snapshots").include("/com/force"),
    ProxiedRepository("/maven-datanucleus", "www.datanucleus.org", "/downloads/maven2", s3prefix + "-proxy-datanucleus").include("/org/datanucleus").include("/javax/jdo"),
    ProxiedRepository("/maven-typesafe-releases", "repo.typesafe.com", "/typesafe/maven-releases", s3prefix + "-proxy-typesafe-releases").include("/com/typesafe"),
    ProxiedRepository("/ivy-typesafe-releases", "repo.typesafe.com", "/typesafe/ivy-releases", s3prefix + "-proxy-typesafe-ivy-releases").include("/com.typesafe").include("/org.scala-tools.sbt"),
    ProxiedRepository("/ivy-typesafe-snapshots", "repo.typesafe.com", "/typesafe/ivy-snapshots", s3prefix + "-proxy-typesafe-ivy-snapshots").include("/com.typesafe").include("/org.scala-tools.sbt"),
    //ProxiedRepository("/maven-scala-tools-releases", "s3.amazonaws.com", "/" + s3prefix + "-proxy-scalatools-releases", s3prefix + "-proxy-scalatools-releases"),
    //ProxiedRepository("/maven-scala-tools-snapshots", "s3.amazonaws.com", "/" + s3prefix + "-proxy-scalatools-snapshots", s3prefix + "-proxy-scalatools-snapshots"),
    ProxiedRepository("/ivy-databinder", "databinder.net", "/repo", s3prefix + "-proxy-databinder").include("/org.scala-tools.sbt"),
    ProxiedRepository("/maven-twitter", "maven.twttr.com", "", s3prefix + "-proxy-twitter").include("/com/twitter"),
    ProxiedRepository("/maven-glassfish", "download.java.net", "/maven/glassfish", s3prefix + "-proxy-glassfish").include("/org/glassfish")
  )
  /*Create the Groups*/
  val all = RepositoryGroup("/jvm", proxies)

  /*Grab AWS keys */
  val s3key = S3Key {
    Properties.envOrNone("S3_KEY").getOrElse {
      System.out.println("S3_KEY env var not defined, exiting")
      System.exit(666)
      "noKey"
    }
  }

  val s3secret = S3Secret {
    Properties.envOrNone("S3_SECRET").getOrElse {
      System.out.println("S3_SECRET env var not defined, exiting")
      System.exit(666)
      "noSecret"
    }
  }

  val doPrimeCaches = Properties.envOrElse("PRIME_CACHES", "false").toBoolean

  def configureLogging() = {
    Logger.clearHandlers()
    val logConf = new LoggerConfig {
      node = ""
      level = Logger.levelNames.get(Properties.envOrElse("LOG_LEVEL", "INFO"))
      handlers = List(new ConsoleHandlerConfig, new NewRelicLogHandlerConfig)
    }
    logConf.apply()
    val supressNettyWarning = new LoggerConfig {
      node = "org.jboss.netty.channel.SimpleChannelHandler"
      level = Logger.WARNING
    }
    supressNettyWarning.apply()
  }

  def main(args: Array[String]) {
    configureLogging()
    val log = Logger.get("S3Server-Main")
    log.warning("Starting S3rver")

    //implicit val stats = NullStatsReceiver
    val stats = NewRelicStatsReceiver

    /*Build the Service*/
    val service = new ProxyService(proxies, List(all), doPrimeCaches, s3key, s3secret, stats)

    /*Grab port to bind to*/
    val address = new InetSocketAddress(Properties.envOrElse("PORT", "8080").toInt)

    /*Build the Server*/
    val server = ServerBuilder()
      .codec(Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabyte))
      .bindTo(address)
      .sendBufferSize(262144)
      .recvBufferSize(262144)
      .maxConcurrentRequests(64)
      .reportTo(stats)
      .name("s3pository")
      .build(service)

    log.warning("S3rver started")
  }
}








