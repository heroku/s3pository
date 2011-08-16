package com.heroku.maven.s3pository


import com.twitter.util.Future
import com.twitter.conversions.storage._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.Http
import com.twitter.logging.Logger
import com.twitter.logging.config.{ConsoleHandlerConfig, LoggerConfig}

import java.net.InetSocketAddress

import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.buffer.ChannelBuffers

import util.Properties
import com.twitter.finagle.stats.{NullStatsReceiver, SummarizingStatsReceiver}



object S3rver {

  val s3prefix = Properties.envOrNone("S3_PREFIX").getOrElse {
		  System.out.println("S3_PREFIX env var not defined, exiting")
		  System.exit(666)
		  "noPrefix"
	  }
  
  /*Wire up the proxied repositories*/
  val proxies = List(/*proxy prefix          source repo host                          source repo path to m2 repo             S3 bucket to store cached content */
    ProxiedRepository("/central",               "repo1.maven.org",                        "/maven2",                              s3prefix + "-proxy-central"),
    ProxiedRepository("/spring-releases",       "maven.springframework.org",              "/release",                             s3prefix + "-proxy-spring-releases").include("/com/springsource").include("/org/springframework").include("/org/aspectj"),
    ProxiedRepository("/spring-milestones",     "maven.springframework.org",              "/milestone",                           s3prefix + "-proxy-spring-milestones").include("/com/springsource").include("/org/springframework").include("/org/aspectj"),
    ProxiedRepository("/spring-roo",            "spring-roo-repository.springsource.org", "/release",                             s3prefix + "-proxy-spring-roo").include("/org/springframework/roo"),
    ProxiedRepository("/jboss",                 "repository.jboss.org",                   "/nexus/content/repositories/releases", s3prefix + "-proxy-jboss", 443, true).include("/jboss").include("/org/jboss").include("/javax/validation").include("/org/hibernate"),
    ProxiedRepository("/sonatype-oss",          "oss.sonatype.org",                       "/content/repositories/snapshots",      s3prefix + "-proxy-sonatype-snapshots").include("/com/force"),
    //ProxiedRepository("/force-releases",        "repo.t.salesforce.com",                  "/archiva/repository/releases",         s3prefix + "-proxy-force-releases"),
    //ProxiedRepository("/force-milestones",      "repo.t.salesforce.com",                  "/archiva/repository/snapshots",        s3prefix + "-proxy-force-snapshots"),
    ProxiedRepository("/datanucleus",           "www.datanucleus.org",                    "/downloads/maven2",                    s3prefix + "-proxy-datanucleus").include("/org/datanucleus").include("/javax/jdo"),
    ProxiedRepository("/typesafe-releases",     "repo.typesafe.com",                      "/typesafe/maven-releases",             s3prefix + "-proxy-typesafe-releases").include("/com/typesafe"),
    ProxiedRepository("/typesafe-releases-ivy", "repo.typesafe.com",                      "/typesafe/ivy-releases",               s3prefix + "-proxy-typesafe-ivy-releases").include("/com.typesafe.sbteclipse").include("/org.scala-tools.sbt"),
    //ProxiedRepository("/typesafe-snapshots",    "repo.typesafe.com",                      "/typesafe/maven-snapshots",            s3prefix + "-proxy-typesafe-snapshots"),
    ProxiedRepository("/scala-tools-releases",  "scala-tools.org",                        "/repo-releases",                       s3prefix + "-proxy-scalatools-releases"),
    ProxiedRepository("/scala-tools-snapshots", "scala-tools.org",                        "/repo-snapshots",                      s3prefix + "-proxy-scalatools-snapshots"),
    ProxiedRepository("/databinder",            "databinder.net",                         "/repo",                                s3prefix + "-proxy-databinder").include("/org.scala-tools.sbt"),
    ProxiedRepository("/twitter",               "maven.twttr.com",                        "",                                     s3prefix + "-proxy-twitter").include("/com/twitter"),
    ProxiedRepository("/glassfish",             "download.java.net",                      "/maven/glassfish",                     s3prefix + "-proxy-glassfish").include("/org/glassfish")
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

    //implicit val stats = NullStatsReceiver
    //implicit val stats = new SummarizingStatsReceiver
    implicit val stats = NewRelicStatsReceiver

    /*Build the Service*/
    val service = new ProxyService(proxies, List(all))
    //val service = new Stats(stats) andThen new ProxyService(proxies, List(all))



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






