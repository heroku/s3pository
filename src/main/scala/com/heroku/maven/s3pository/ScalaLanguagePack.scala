package com.heroku.maven.s3pository

import com.twitter.finagle.stats.NullStatsReceiver
import com.heroku.maven.s3pository.ProxyService._
import com.heroku.maven.s3pository.S3rver._

object ScalaOn {
  val from11 = "/sbt-0.11.0.boot.properties.proxy"
  val plugin = "/Heroku-0.11.0.scala.proxy"
  val conf = "/heroku-plugins-0.11.0.sbt.proxy"

  def main(args: Array[String]) {
    ScalaSwitch.doSwitch(from11, plugin, conf)
  }
}

object ScalaOff {
  val from11 = "/sbt-0.11.0.boot.properties.noproxy"
  val plugin = "/Heroku-0.11.0.scala.noproxy"
  val conf = "/heroku-plugins-0.11.0.sbt.noproxy"


  def main(args: Array[String]) {
    ScalaSwitch.doSwitch(from11, plugin, conf)
  }
}

object ScalaSwitch {
  val bucket = s3prefix + "-langpack-scala"
  val to11 = "/sbt-0.11.0.boot.properties"
  val plug11 = "/Heroku-0.11.0.scala"
  val conf11 = "/heroku-plugins-0.11.0.sbt"

  def doSwitch(from11: String, plugin11: String, plugConf11: String) {
    implicit val stats = NullStatsReceiver
    val client = clientService(bucket + ".s3.amazonaws.com", 80, false, "s3 client for:" + bucket)
    val request11 = put(to11).headers(COPY_SOURCE -> ("/" + bucket + from11), ACL -> "public-read").s3headers(bucket)
    val response11 = client.service(request11).get()
    val requestPlugin11 = put(plug11).headers(COPY_SOURCE -> ("/" + bucket + plugin11), ACL -> "public-read").s3headers(bucket)
    val responsePlugin11 = client.service(requestPlugin11).get()
    val requestPlugConf11 = put(conf11).headers(COPY_SOURCE -> ("/" + bucket + plugConf11), ACL -> "public-read").s3headers(bucket)
    val responsePlugConf11 = client.service(requestPlugConf11).get()
    println(response11.getStatus.getReasonPhrase)
    println(response11.getContent.toString("UTF-8"))
    println(responsePlugin11.getStatus.getReasonPhrase)
    println(responsePlugin11.getContent.toString("UTF-8"))
    println(responsePlugConf11.getStatus.getReasonPhrase)
    println(responsePlugConf11.getContent.toString("UTF-8"))
    client.release()
  }

}
