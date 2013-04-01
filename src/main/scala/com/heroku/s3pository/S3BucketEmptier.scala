package com.heroku.s3pository

import com.heroku.s3pository.S3rver._
import com.heroku.s3pository.S3Updater._
import com.twitter.util.Future
import com.twitter.logging.Logger
import com.twitter.logging.config.{ConsoleHandlerConfig, LoggerConfig}


import util.Properties
import com.heroku.finagle.aws.{Delete, ListBucket, S3}

object S3BucketEmptier {

  lazy val log = Logger.get("S3BucketEmptier")

  def main(args: Array[String]) {
    Logger.clearHandlers()
    val logConf = new LoggerConfig {
      node = ""
      level = Logger.levelNames.get(Properties.envOrElse("LOG_LEVEL", "INFO"))
      handlers = List(new ConsoleHandlerConfig)
    }
    logConf.apply()
    val supressNettyWarning = new LoggerConfig {
      node = "org.jboss.netty.channel.SimpleChannelHandler"
      level = Logger.ERROR
    }
    supressNettyWarning.apply()
    log.warning("Starting S3BucketEmptier")
    val s3Client = S3.client(s3key, s3secret)
    args.headOption foreach {
      bucket => {
        val keys = ListBucket.getKeys(s3Client, bucket)
        val futures = keys.filter(args.tail.size == 0 || contains(_, args.tail.toList)) map {
          key => {
            println("Delete:" + key)
            s3Client(Delete(bucket, key))
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
    System.exit(0)
  }

}