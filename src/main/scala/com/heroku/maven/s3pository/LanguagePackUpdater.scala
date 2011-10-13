package com.heroku.maven.s3pository

import com.heroku.maven.s3pository.ProxyService._
import com.heroku.maven.s3pository.S3rver._
import com.twitter.finagle.stats.NullStatsReceiver
import scala.io._
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

object LangPackSwitch {

  def doSwitch(bucket: String, from: String, to: String) {
    implicit val stats = NullStatsReceiver
    val client = clientService(bucket + ".s3.amazonaws.com", 80, false, "s3 client for:" + bucket)
    val request = put(to).headers(COPY_SOURCE -> ("/" + bucket + from), ACL -> "public-read").s3headers(bucket)
    val response = client.service(request).get()
    println(response.getStatus.getReasonPhrase)
    println(response.getContent.toString("UTF-8"))
    client.release()
  }

}

object LangPackUpdate {

  def readFile(fileName: String) = {
    ChannelBuffers.wrappedBuffer(Source.fromFile(fileName).mkString.getBytes)
  }

  def putFile(bucket: String, fileName: String, content: ChannelBuffer) {
    implicit val stats = NullStatsReceiver
    val client = clientService(bucket + ".s3.amazonaws.com", 80, false, "s3 client for:" + bucket)
    val s3put = put(fileName).headers(CONTENT_LENGTH -> content.readableBytes.toString, STORAGE_CLASS -> RRS,
      CONTENT_TYPE -> "application/xml").s3headers(bucket)
    s3put.setContent(content)
    val response = client.service(s3put).get()
    println(response.toString)
    println(response.getContent.toString("UTF-8"))
    client.release()
  }
}

object Off {

  def main(args: Array[String]) {
    JavaOff.main(args)
    PlayOff.main(args)
    ScalaOff.main(args)
  }

}

object On {

  def main(args: Array[String]) {
    JavaOn.main(args)
    PlayOn.main(args)
    ScalaOn.main(args)
  }

}


object Update {

  def main(args: Array[String]) {
    JavaUpdate.main(args)
    PlayUpdate.main(args)
  }

}

