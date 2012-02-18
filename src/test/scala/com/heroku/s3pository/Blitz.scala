package com.heroku.s3pository

import com.heroku.s3pository.S3rver._
import io.blitz.curl.Rush
import java.net.URL
import io.blitz.curl.config.variable.ListVariable
import collection.JavaConversions._
import io.blitz.curl.rush.{RushResult, IRushListener}
import io.blitz.curl.config.{Pattern, Interval}
import util.{Random, Properties}
import com.heroku.finagle.aws.{ListBucket, S3}

object Blitz {

  def main(args: Array[String]) {
    val user = Properties.envOrNone("BLITZ_API_USER").getOrElse {
      println("No User")
      System.exit(666)
      "noUser"
    }
    val key = Properties.envOrNone("BLITZ_API_KEY").getOrElse {
      println("No KEy")
      System.exit(666)
      "noKey"
    }

    val host = Properties.envOrNone("BLITZ_HOSTNAME").getOrElse {
      println("No KEy")
      System.exit(666)
      "noKey"
    }

    val listClient = S3.client(s3key, s3secret)

    val keys = proxies.foldLeft(List.empty[String]) {
      (l, p) =>
        val keys = ListBucket.getKeys(listClient, p.bucket)
        l ++ keys
    }
    listClient.release()

    val rush = new Rush(user, key)
    rush.setUrl(new URL("http://" + host + "/" + all.prefix + "/#{key}"))
    rush.setTimeout(5000)
    rush.setRegion("virginia")
    val keyVar = new ListVariable(asJavaList(Random.shuffle(keys)))
    val vars = Map("key" -> keyVar)
    rush.setVariables(vars)
    val intervals = List(new Interval(100, 500, 60))
    rush.setPattern(new Pattern(asJavaList(intervals)))
    rush.addListener(new IRushListener {
      def onData(res: RushResult) = {
        println("success")

        res.getTimeline.foreach {
          p =>
            printf("""
             |  Duration: %s
             |  Hits: %d
             |  Errors: %d
             |  Timeouts: %d
             |  Total: %d
             |  Volume: %d
             |  Bytes Rec: %d
             |
             """, p.getDuration.toString, p.getHits, p.getErrors, p.getTimeouts, p.getTotal, p.getVolume, p.getRxBytes)
        }
        true
      }
    })
    rush.execute()
  }


}