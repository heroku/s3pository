package com.heroku.maven.s3pository

import com.twitter.finagle.stats.NullStatsReceiver
import com.heroku.maven.s3pository.ProxyService._
import com.heroku.maven.s3pository.S3rver._

object ScalaOn {
  val from7 = "/sbt-0.7.7.boot.properties.proxy"
  val from10 = "/sbt-0.10.1.boot.properties.proxy"

  def main(args: Array[String]) {
    ScalaSwitch.doSwitch(from7,from10)
  }
}

object ScalaOff {
  val from7 = "/sbt-0.7.7.boot.properties.noproxy"
  val from10 = "/sbt-0.10.1.boot.properties.noproxy"

  def main(args: Array[String]) {
    ScalaSwitch.doSwitch(from7,from10)
  }
}

object ScalaSwitch {
  val bucket = s3prefix + "-langpack-scala"
  val to7 = "/sbt-0.7.7.boot.properties"
  val to10 = "/sbt-0.10.1.boot.properties"

  def doSwitch(from7: String, from10:String) {
    implicit val stats = NullStatsReceiver
    val client = clientService(bucket + ".s3.amazonaws.com", 80, false, "s3 client for:" + bucket)
    val request7 = put(to7).headers(COPY_SOURCE -> ("/" + bucket + from7), ACL -> "public-read").s3headers(bucket)
    val response7 = client.service(request7).get()
    val request10 = put(to10).headers(COPY_SOURCE -> ("/" + bucket + from10), ACL -> "public-read").s3headers(bucket)
    val response10 = client.service(request10).get()
    println(response7.getStatus.getReasonPhrase)
    println(response7.getContent.toString("UTF-8"))
    println(response10.getStatus.getReasonPhrase)
    println(response10.getContent.toString("UTF-8"))
    client.release()
  }

}