package com.heroku.maven.s3pository

import com.twitter.conversions.storage._
import com.twitter.finagle.http.Http
import com.twitter.finagle.builder.ServerBuilder

import java.net.InetSocketAddress
import java.util.logging.{LogManager, Logger}

import util.Properties


object S3rver {
  def main(args: Array[String]) {
    LogManager.getLogManager.readConfiguration(getClass.getClassLoader.getResourceAsStream("logging.properties"))
    val log = Logger.getLogger("S3Server-Main")
    log.info("Starting S3rver")
    /*Wire up the proxied repositories*/
    val proxies = List(/*proxy prefix          source repo host                          source repo path to m2 repo             S3 bucket to store cached content */
      ProxiedRepository("/central",            "repo1.maven.org",                        "/maven2",                              "sclasen-proxy-central"),
      ProxiedRepository("/spring-releases",    "maven.springframework.org",              "/release",                             "sclasen-proxy-spring-releases"),
      ProxiedRepository("/spring-milestones",  "maven.springframework.org",              "/milestone",                           "sclasen-proxy-spring-milestones"),
      ProxiedRepository("/spring-milestones",  "spring-roo-repository.springsource.org", "/release",                             "sclasen-proxy-spring-roo"),
      ProxiedRepository("/jboss",              "repository.jboss.org",                   "/nexus/content/repositories/releases", "sclasen-proxy-jboss", 443, true),
      ProxiedRepository("/force-releases",     "repo.t.salesforce.com",                  "/archiva/repository/releases",         "sclasen-proxy-force-releases"),
      ProxiedRepository("/force-milestones",   "repo.t.salesforce.com",                  "/archiva/repository/snapshots",        "sclasen-proxy-force-snapshots"),
      ProxiedRepository("/datanucleus",        "www.datanucleus.org",                    "/downloads/maven2",                    "sclasen-proxy-datanucleus"),
      ProxiedRepository("/typesafe-releases",  "repo.typesafe.com",                      "/typesafe/maven-releases",             "sclasen-proxy-typesafe-releases"),
      ProxiedRepository("/typesafe-snapshots", "repo.typesafe.com",                      "/typesafe/maven-snapshots",            "sclasen-proxy-typesafe-snapshots")
    )
    /*Create the Groups*/
    val all = RepositoryGroup("/all", proxies)

    /*Grab AWS keys */
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
    /*Build the Service*/
    val service = new ProxyService(proxies, List(all), s3key, s3secret)

    /*Grab port to bind to*/
    val address = new InetSocketAddress(Properties.envOrElse("PORT", "8080").toInt)

    /*Build the Server*/
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







