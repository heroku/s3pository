package com.heroku.maven.s3pository

import com.heroku.maven.s3pository.ProxyService._
import com.heroku.maven.s3pository.S3rver._
import com.twitter.finagle.stats.NullStatsReceiver
import scala.io._
import util.Properties
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

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
  val bucket = s3prefix + "-langpack-java"
  val to = "/settings.xml"

  def doSwitch(from: String) {
    implicit val stats = NullStatsReceiver
    val client = clientService(bucket + ".s3.amazonaws.com", 80, false, "s3 client for:" + bucket)
    val request = put(to).headers(COPY_SOURCE -> ("/" + bucket + from), ACL -> "public-read").s3headers(bucket)
    val response = client.service(request).get()
    println(response.getStatus.getReasonPhrase)
    println(response.getContent.toString("UTF-8"))
    client.release()
  }

}

object Update {
  
  val bucket = s3prefix + "-langpack-java"
  
  def readFile(fileName: String) = {
    ChannelBuffers.wrappedBuffer(Source.fromFile(fileName).mkString.getBytes)
  }
  
  def putFile(fileName: String, content: ChannelBuffer) {
	  implicit val stats = NullStatsReceiver
	  val client = clientService(bucket + ".s3.amazonaws.com", 80, false, "s3 client for:" + bucket)
	  val s3put = put(fileName).headers(CONTENT_LENGTH -> content.readableBytes.toString, STORAGE_CLASS -> RRS,
			  CONTENT_TYPE -> "application/xml").s3headers(bucket)
	  s3put.setContent(content)
	  val response = client.service(s3put).get()
	  println(s3put.toString)
	  println("")
	  println(response.toString)
	  println(response.getContent.toString("UTF-8"))
	  client.release()    
  }
  
  def main(args: Array[String]) {
	  val settingsProxyBuffer = readFile("settings-proxy.xml");
	  val settingsNoProxyBuffer = readFile("settings-noproxy.xml")
	  putFile("/settings-proxy.xml", settingsProxyBuffer)
	  putFile("/settings-noproxy.xml", settingsNoProxyBuffer)
  }
}