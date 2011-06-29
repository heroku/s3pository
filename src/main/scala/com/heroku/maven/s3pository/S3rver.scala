package com.heroku.maven.s3pository

import com.twitter.finagle.http.Http
import java.net.InetSocketAddress
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.conversions.storage._
import util.Properties
import java.util.logging.{LogManager, Logger}

object S3rver {
  def main(args: Array[String]) {
    LogManager.getLogManager.readConfiguration(getClass.getClassLoader.getResourceAsStream("logging.properties"))
    val log = Logger.getLogger("S3Server-Main")
    log.info("Starting S3rver")
    val central = ProxiedRepository("/central", "repo1.maven.org", "/maven2", "sclasen-proxy-central")
    val springReleases = ProxiedRepository("/spring-releases", "maven.springframework.org", "/release", "sclasen-proxy-spring-releases")
    val springMilestones = ProxiedRepository("/spring-milestones", "maven.springframework.org", "/milestone", "sclasen-proxy-spring-milestones")
    val springRoo = ProxiedRepository("/spring-milestones", "spring-roo-repository.springsource.org", "/release", "sclasen-proxy-spring-roo")
    val jboss = ProxiedRepository("/jboss", "repository.jboss.org", "/nexus/content/repositories/releases", "sclasen-proxy-jboss", 443, true)
    val forceReleases = ProxiedRepository("/force-releases", "repo.t.salesforce.com", "/archiva/repository/releases", "sclasen-proxy-force-releases")
    val forceSnapshots = ProxiedRepository("/force-milestones", "repo.t.salesforce.com", "/archiva/repository/snapshots", "sclasen-proxy-force-snapshots")
    val datanucleus = ProxiedRepository("/datanucleus", "www.datanucleus.org", "downloads/maven2", "sclasen-proxy-datanucleus")
    val typesafe = ProxiedRepository("/typesafe-releases", "repo.typesafe.com", "/typesafe/maven-releases", "sclasen-proxy-typesafe-releases")
    val typesafeSnapshots = ProxiedRepository("/typesafe-snapshots", "repo.typesafe.com", "/typesafe/maven-snapshots", "sclasen-proxy-typesafe-snapshots")
    val proxies = List(central, springReleases,springMilestones,springRoo,jboss,forceReleases,forceSnapshots,datanucleus,typesafe,typesafeSnapshots)
    val all = RepositoryGroup("/all", proxies)
    val address = new InetSocketAddress(Properties.envOrElse("PORT", "8080").toInt)
    val s3key: String = Properties.envOrNone("S3_KEY").getOrElse {
      log.severe("S3_KEY env var not defined, exiting")
      System.exit(666)
      "noKey"
    }
    val s3secret: String = Properties.envOrNone("S3_SECRET").getOrElse {
      log.severe("S3_SECRET env var not defined, exiting")
      System.exit(666)
      "noSecret"
    }
    val service = new ProxyService(proxies, List(all), s3key, s3secret)
    val server = ServerBuilder()
      .codec(Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabyte))
      .bindTo(address)
      .sendBufferSize(1048576)
      .recvBufferSize(1048576)
      .name("s3pository")
      .logger(Logger.getLogger("finagle.server"))
      .build(service)

    log.info("S3rver started")
  }
}







