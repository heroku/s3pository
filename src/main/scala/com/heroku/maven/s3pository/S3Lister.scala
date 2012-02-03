package com.heroku.maven.s3pository


import com.heroku.maven.s3pository.S3rver._
import com.heroku.maven.s3pository.ProxyService._

import com.twitter.logging.config.{ConsoleHandlerConfig, LoggerConfig}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Time

import java.net.InetSocketAddress

import org.jboss.netty.handler.codec.http._

import util.Properties

/*checks for updated artifacts in source repos*/
object S3Lister {

  type Client = Service[HttpRequest, HttpResponse]
  lazy val log = Logger.get("S3Server-Lister")
  lazy val stats = new SummarizingStatsReceiver

  def main(args: Array[String]) {
    Logger.clearHandlers()
    val logConf = new LoggerConfig {
      node = ""
      level = Logger.levelNames.get(Properties.envOrElse("UPDATER_LOG_LEVEL", "INFO"))
      handlers = List(new ConsoleHandlerConfig, new NewRelicLogHandlerConfig)
    }
    logConf.apply()
    val supressNettyWarning = new LoggerConfig {
      node = "org.jboss.netty.channel.SimpleChannelHandler"
      level = Logger.ERROR
    }
    supressNettyWarning.apply()
    log.warning("Starting Lister")
    val s3Client = client("s3.amazonaws.com")
    val start = Time.now
    proxies foreach {
      proxy: ProxiedRepository => {
        val sourceClient = client(proxy)

        val keys = getKeys(s3Client, proxy.bucket)
        println(proxy.bucket + " has " + keys.size)
        keys.foreach(println)
        sourceClient.release()


      }
    }

    s3Client.release()
    val end = Time.now - start
    log.warning("total time (seconds) %d", end.inSeconds)
    log.warning(stats.summary)
    System.exit(0)
  }

  def contains(key: String, filters: List[String]): Boolean = {
    filters.headOption match {
      case None => false
      case Some(filter) => {
        key.contains(filter) || contains(key, filters.tail)
      }
    }
  }


  def client(repo: ProxiedRepository): Client = {
    client(repo.host, repo.port, repo.ssl)
  }

  def client(host: String, port: Int = 80, ssl: Boolean = false): Client = {
    var builder = ClientBuilder()
      .codec(Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabyte))
      .sendBufferSize(1048576)
      .recvBufferSize(1048576)
      .hosts(new InetSocketAddress(host, port))
      .hostConnectionLimit(16)
      .keepAlive(true)
      .hostConnectionMaxWaiters(Integer.MAX_VALUE)
      .requestTimeout(30.seconds)
      .connectionTimeout(5.seconds)
      .name(host)
      .reportTo(stats)
    if (ssl) (builder = builder.tlsWithoutValidation())
    builder.build()
  }
}