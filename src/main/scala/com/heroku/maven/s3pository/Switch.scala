package com.heroku.maven.s3pository

import com.heroku.maven.s3pository.ProxyService._
import com.heroku.maven.s3pository.S3rver._
import com.twitter.finagle.stats.NullStatsReceiver

object On {
  val from = "/settings-proxy.xml"

  def main(args: Array[String]) {
    Switch.doSwitch(from)
  }
}

object Off {
  val from = "/settings-noproxy.xml"

  def main(args: Array[String]) {
    Switch.doSwitch(from)
  }
}

object Switch {
  val bucket = "sclasen-langpack-java"
  val to = "/settings.xml"

  def doSwitch(from: String) {
    implicit val stats = NullStatsReceiver
    val client = clientService(bucket + ".s3.amazonaws.com", 80, false, "s3 client for:" + bucket)
    val request = put(to).headers(Map(COPY_SOURCE -> ("/" + bucket + from))).s3headers(bucket)
    val response = client.service(request).get()
    println(response.getStatus.getReasonPhrase)
    println(response.getContent.toString("UTF-8"))
    val verify = get(to).s3headers(bucket)
    val vresp = client.service(verify).get()
    println("Current %s".format(to))
    println(vresp.getContent.toString("UTF-8"))
    client.release()
  }

}