package com.heroku.maven.s3pository

import com.twitter.finagle.http.Http
import java.net.InetSocketAddress
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.conversions.storage._
import util.Properties
import collection.mutable.{HashMap => MMap}
import java.util.logging.{LogManager, Logger}
object S3rver {
  def main(args: Array[String]) {
    LogManager.getLogManager.readConfiguration(getClass.getClassLoader.getResourceAsStream("logging.properties"))
    val log = Logger.getLogger("S3Server-Main")
    val central = ProxiedRepository("/central", "repo1.maven.org", "/maven2", "sclasen-proxy-central")
    val akka = ProxiedRepository("/akka", "akka.io", "/repository", "sclasen-proxy-akka")
    val proxies = List(central, akka)
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
      .logger(Logger.getLogger("S3Server"))
      .build(service)
  }
}







