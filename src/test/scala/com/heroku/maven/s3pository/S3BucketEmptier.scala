package com.heroku.maven.s3pository

import com.heroku.maven.s3pository.S3rver._
import com.heroku.maven.s3pository.S3Updater._
import com.twitter.util.Future
import com.twitter.logging.Logger
import com.twitter.logging.config.{ConsoleHandlerConfig, LoggerConfig}

import org.jboss.netty.handler.codec.http.HttpHeaders.Names._


import util.Properties

object S3BucketEmptier {

  lazy val log = Logger.get("S3BucketEmptier")

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
      level = Logger.ERROR
    }
    supressNettyWarning.apply()
    log.warning("Starting S3BucketEmptier")
    val s3Client = client("s3.amazonaws.com")
    args foreach {
      bucket => {
        val keys = getKeys(s3Client, bucket)
        val futures = keys map {
          key => {
            val req = delete("/" + key).s3headers(s3key, s3secret, bucket)
            s3Client(req)
          }
        }
        Future.collect(futures).get().foreach {
          resp => {
            log.info("%s %s", resp.getStatus.getReasonPhrase, resp.getContent.toString("UTF-8"))
          }
        }
      }
    }
    s3Client.release()
  }

}