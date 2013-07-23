package com.heroku.s3pository


import com.twitter.conversions.storage._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.Http
import com.twitter.logging.Logger
import com.twitter.logging.config.{ConsoleHandlerConfig, LoggerConfig}

import java.net.InetSocketAddress

import util.Properties
import com.heroku.finagle.aws.S3.{S3Secret, S3Key}
import com.twitter.finagle.stats.NullStatsReceiver


object S3rver {

  val s3prefix = Properties.envOrNone("S3_PREFIX").getOrElse {
    System.out.println("S3_PREFIX env var not defined, exiting")
    System.exit(666)
    "noPrefix"
  }
  /*proxy prefix          source repo host                          source repo path to m2 repo             S3 bucket to store cached content */
  val mavenCentral = ProxiedRepository("/maven-central", "repo1.maven.org", "/maven2", s3prefix + "-proxy-central")
  val mavenSpringReleases = ProxiedRepository("/maven-spring-releases", "maven.springframework.org", "/release", s3prefix + "-proxy-spring-releases").include("/com/springsource").include("/org/springframework").include("/org/aspectj")
  val mavenSpringMilestones = ProxiedRepository("/maven-spring-milestones", "maven.springframework.org", "/milestone", s3prefix + "-proxy-spring-milestones").include("/com/springsource").include("/org/springframework").include("/org/aspectj")
  val mavenSpringRoo = ProxiedRepository("/maven-spring-roo", "spring-roo-repository.springsource.org", "/release", s3prefix + "-proxy-spring-roo").include("/org/springframework/roo")
  val mavenJboss = ProxiedRepository("/maven-jboss", "repository.jboss.org", "/nexus/content/repositories/releases", s3prefix + "-proxy-jboss", 443, true).include("/jboss").include("/org/jboss").include("/javax/validation").include("/org/hibernate")
  val mavenJbossPublic = ProxiedRepository("/maven-jboss-public", "repository.jboss.org", "/nexus/content/groups/public", s3prefix + "-proxy-jboss-public", 443, true).include("/jboss").include("/org/jboss").include("/javax/validation").include("/org/hibernate")
  val mavenJbossThirdparty = ProxiedRepository("/maven-jboss-thirdparty", "repository.jboss.org", "/nexus/content/repositories/thirdparty-uploads", s3prefix + "-proxy-jboss-thirdparty", 443, true)
  val mavenSonatypeOss = ProxiedRepository("/maven-sonatype-oss", "oss.sonatype.org", "/content/repositories/snapshots", s3prefix + "-proxy-sonatype-snapshots").include("/com/force").include("/com/heroku")
  val mavenSonatypeOssScalaTools = ProxiedRepository("/maven-scala-tools-releases", "oss.sonatype.org", "/content/groups/scala-tools", s3prefix + "-proxy-sonatype-scala-tools")
  val mavenDatanucleus = ProxiedRepository("/maven-datanucleus", "www.datanucleus.org", "/downloads/maven2", s3prefix + "-proxy-datanucleus").include("/org/datanucleus").include("/javax/jdo")
  val mavenTypesafeReleases = ProxiedRepository("/maven-typesafe-releases", "repo.typesafe.com", "/typesafe/maven-releases", s3prefix + "-proxy-typesafe-releases").include("/com/typesafe")
  val ivyTypesafeReleases = ProxiedRepository("/ivy-typesafe-releases", "repo.typesafe.com", "/typesafe/ivy-releases", s3prefix + "-proxy-typesafe-ivy-releases").include("/com.typesafe").include("/org.scala-tools.sbt").include("/org.scala-sbt")
  val ivyTypesafeSnapshots = ProxiedRepository("/ivy-typesafe-snapshots", "repo.typesafe.com", "/typesafe/ivy-snapshots", s3prefix + "-proxy-typesafe-ivy-snapshots").include("/com.typesafe").include("/org.scala-tools.sbt").include("/org.scala-sbt")
  val ivyDatabinder = ProxiedRepository("/ivy-databinder", "databinder.net", "/repo", s3prefix + "-proxy-databinder").include("/org.scala-tools.sbt")
  val mavenTwitter = ProxiedRepository("/maven-twitter", "maven.twttr.com", "", s3prefix + "-proxy-twitter").include("/com/twitter")
  val mavenGlassfish = ProxiedRepository("/maven-glassfish", "download.java.net", "/maven/glassfish", s3prefix + "-proxy-glassfish").include("/org/glassfish")
  val grailsCentral = ProxiedRepository("/grails-central", "repo.grails.org", "/grails/core", s3prefix + "-proxy-grails-core")
  val grailsPlugins = ProxiedRepository("/grails-plugins", "repo.grails.org", "/grails/plugins", s3prefix + "-proxy-grails-plugins").include("/org/grails/plugins")
  val clojars = ProxiedRepository("/clojars", "clojars.org", "/repo", s3prefix + "-proxy-clojars")
  val node = ProxiedRepository("/node", "nodejs.org", "/dist", s3prefix + "-proxy-node")
  //val grailsPluginsSvn = ProxiedRepository("/grails-plugins-svn", "plugins.grails.org", "", s3prefix + "-proxy-grails-plugins-svn")

  /*Wire up the proxied repositories*/
  val proxies = List(
    mavenCentral,
    mavenSpringReleases,
    mavenSpringMilestones,
    mavenSpringRoo,
    mavenJboss,
    mavenJbossPublic,
    mavenJbossThirdparty,
    mavenSonatypeOss,
    mavenSonatypeOssScalaTools,
    mavenDatanucleus,
    mavenTypesafeReleases,
    ivyTypesafeReleases,
    ivyTypesafeSnapshots,
    ivyDatabinder,
    mavenTwitter,
    mavenGlassfish,
    clojars,
    node
  )

  /*Wire up the proxied repositories for Grails*/
  val grailsProxies = List(
    mavenCentral,
    mavenSpringReleases,
    mavenSpringMilestones,
    mavenJboss,
    mavenSonatypeOss,
    mavenTwitter,
    grailsCentral,
    grailsPlugins
  )

  val clojureProxies = List(
    mavenCentral,
    clojars,
    mavenSonatypeOss
  )

  /*Create the Groups*/
  val all = RepositoryGroup("/jvm", proxies)
  val grails = RepositoryGroup("/grails", grailsProxies, 360)
  val clojure = RepositoryGroup("/clojure", clojureProxies, 360)

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
      handlers = List(new ConsoleHandlerConfig)
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

    val stats = NullStatsReceiver

    /*Build the Service*/
    val service = new ProxyService(proxies ++ grailsProxies ++ clojureProxies, List(all, grails, clojure), doPrimeCaches, s3key, s3secret, stats)

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

    log.info("S3rver started (PORT " + address.getPort + ")")
  }
}








