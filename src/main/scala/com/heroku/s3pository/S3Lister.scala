package com.heroku.s3pository


import com.heroku.s3pository.S3rver._

import com.twitter.finagle.Service
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Time


import org.jboss.netty.handler.codec.http._

import com.heroku.finagle.aws.{ListBucket, S3}

/*checks for updated artifacts in source repos*/
object S3Lister {

  type Client = Service[HttpRequest, HttpResponse]
  lazy val log = Logger.get("S3Server-Lister")
  lazy val stats = new SummarizingStatsReceiver

  def main(args: Array[String]) {
    configureLogging()
    log.warning("Starting Lister")
    val s3Client = S3.client(s3key, s3secret)
    val start = Time.now
    proxies foreach {
      proxy: ProxiedRepository => {
        val keys = ListBucket.getKeys(s3Client, proxy.bucket)
        println(proxy.bucket + " has " + keys.size)
        keys.foreach(println)
      }
    }
    s3Client.release()
    val end = Time.now - start
    log.warning("total time (seconds) %d", end.inSeconds)
    log.warning(stats.summary)
    System.exit(0)
  }


}