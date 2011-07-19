package com.heroku.maven.s3pository



import com.twitter.conversions.storage._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.logging.Logger
import com.twitter.logging.config.{ConsoleHandlerConfig, LoggerConfig}

import java.net.InetSocketAddress

import util.Properties
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.stats.{StatsReceiver, SummarizingStatsReceiver}
import com.twitter.finagle.http.Http
import org.jboss.netty.handler.codec.http.{DefaultHttpResponse, HttpRequest, HttpResponse}
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.util.Future

object S3rver {

  /*Wire up the proxied repositories*/
  val proxies = List(/*proxy prefix          source repo host                          source repo path to m2 repo             S3 bucket to store cached content */
    ProxiedRepository("/central",               "repo1.maven.org",                        "/maven2",                              "sclasen-proxy-central"),
    ProxiedRepository("/spring-releases",       "maven.springframework.org",              "/release",                             "sclasen-proxy-spring-releases"),
    ProxiedRepository("/spring-milestones",     "maven.springframework.org",              "/milestone",                           "sclasen-proxy-spring-milestones"),
    ProxiedRepository("/spring-milestones",     "spring-roo-repository.springsource.org", "/release",                             "sclasen-proxy-spring-roo"),
    ProxiedRepository("/jboss",                 "repository.jboss.org",                   "/nexus/content/repositories/releases", "sclasen-proxy-jboss", 443, true),
    ProxiedRepository("/force-releases",        "repo.t.salesforce.com",                  "/archiva/repository/releases",         "sclasen-proxy-force-releases"),
    ProxiedRepository("/force-milestones",      "repo.t.salesforce.com",                  "/archiva/repository/snapshots",        "sclasen-proxy-force-snapshots"),
    ProxiedRepository("/datanucleus",           "www.datanucleus.org",                    "/downloads/maven2",                    "sclasen-proxy-datanucleus"),
    ProxiedRepository("/typesafe-releases",     "repo.typesafe.com",                      "/typesafe/maven-releases",             "sclasen-proxy-typesafe-releases"),
    ProxiedRepository("/typesafe-snapshots",    "repo.typesafe.com",                      "/typesafe/maven-snapshots",            "sclasen-proxy-typesafe-snapshots"),
    ProxiedRepository("/scala-tools-releases",  "scala-tools.org",                        "/repo-releases",                       "sclasen-proxy-scalatools-releases"),
    ProxiedRepository("/scala-tools-snapshots", "scala-tools.org",                        "/repo-snapshots",                      "sclasen-proxy-scalatools-snapshots"),
    ProxiedRepository("/databinder",            "databinder.net",                         "/repo",                                "sclasen-proxy-databinder")
  )
  /*Create the Groups*/
  val all = RepositoryGroup("/all", proxies)

  /*Grab AWS keys */
  implicit val s3key = S3Key {
    Properties.envOrNone("S3_KEY").getOrElse {
      System.out.println("S3_KEY env var not defined, exiting")
      System.exit(666)
      "noKey"
    }
  }

  implicit val s3secret = S3Secret {
    Properties.envOrNone("S3_SECRET").getOrElse {
      System.out.println("S3_SECRET env var not defined, exiting")
      System.exit(666)
      "noSecret"
    }
  }


  def main(args: Array[String]) {
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
    val log = Logger.get("S3Server-Main")
    log.warning("Starting S3rver")

    implicit val stats = new SummarizingStatsReceiver

    /*Build the Service*/
    val service = new Stats(stats) andThen new ProxyService(proxies, List(all))



    /*Grab port to bind to*/
    val address = new InetSocketAddress(Properties.envOrElse("PORT", "8080").toInt)

    /*Build the Server*/
    val server = ServerBuilder()
      .codec(Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabyte))
      .bindTo(address)
      .sendBufferSize(1048576)
      .recvBufferSize(1048576)
      //.reportTo(NewRelicStatsReceiver)
      .name("s3pository")
      .build(service)

    log.warning("S3rver started")
  }
}

class Stats(rec:SummarizingStatsReceiver) extends SimpleFilter[HttpRequest, HttpResponse] {
  def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {
    if(request.getUri.equals("/stats")){
      val resp = ok()
      resp.setHeader(CONTENT_TYPE, "text/plain")
      resp.setContent(ChannelBuffers.wrappedBuffer(rec.summary.getBytes("UTF-8")))
      Future.value(resp)
    } else {
       service(request)
    }
  }
}






