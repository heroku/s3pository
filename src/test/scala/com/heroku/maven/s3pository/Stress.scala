package com.heroku.maven.s3pository

import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import java.net.{InetSocketAddress, URI}
import com.twitter.util.{Time, Future, MapMaker}

object Stress {
  def main(args: Array[String]) {
    val uri = new URI(args(0))
    val concurrency = args(1).toInt
    val totalRequests = args(2).toInt

    val errors = new AtomicInteger(0)
    val responses = MapMaker[HttpResponseStatus, AtomicInteger] { config =>
      config.compute { k =>
        new AtomicInteger(0)
      }
    }


    val statsReceiver = new SummarizingStatsReceiver

    val client: Service[HttpRequest, HttpResponse] = ClientBuilder()
      .codec(Http())
      .hosts(new InetSocketAddress(uri.getHost, uri.getPort))
      .hostConnectionCoresize(concurrency)
      .reportTo(statsReceiver)
      .retries(3)
      .hostConnectionLimit(concurrency)
      .build()

    val completedRequests = new AtomicInteger(0)

    val requests = Future.parallel(concurrency) {
      Future.times(totalRequests / concurrency) {
        val request =  new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://maven-s3pository.herokuapp.com/all/org/springframework/spring-context/3.0.5.RELEASE/spring-context-3.0.5.RELEASE.jar")
        HttpHeaders.setHost(request, uri.getHost)

        client( request
        ) onSuccess { response =>
          val respBytes = new Array[Byte](700000)
          response.getContent.readBytes(respBytes)
          responses(response.getStatus).incrementAndGet()
        } handle { case e =>
          errors.incrementAndGet()
        } ensure {
          completedRequests.incrementAndGet()
        }
      }
    }

    val start = Time.now

    Future.join(requests) ensure {
      client.release()

      val duration = start.untilNow
      println("%20s\t%s".format("Status", "Count"))
      for ((status, stat) <- responses)
        println("%20s\t%d".format(status, stat.get))
      println("================")
      println("%d requests completed in %dms (%f requests per second)".format(
        completedRequests.get, duration.inMilliseconds,
        totalRequests.toFloat / duration.inMillis.toFloat * 1000))
      println("%d errors".format(errors.get))

      println("stats")
      println("=====")

      statsReceiver.print()
    }
  }

}