package com.heroku.maven

import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._

import javax.crypto.spec.SecretKeySpec
import javax.crypto.Mac

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, DateTime}

import com.twitter.logging.Logger
import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.ServiceFactory
import com.twitter.util.Future
import java.lang.ref.ReferenceQueue

package object s3pository {

  lazy val log = Logger.get("s3pository")

  /*DateTime format required by AWS*/
  lazy val format = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z").withLocale(java.util.Locale.US).withZone(DateTimeZone.forOffsetHours(0))

  /*headers used by this app that need to be used in signing*/
  val SOURCE_ETAG = "x-amz-meta-source-etag"
  val SOURCE_MOD = "x-amz-meta-source-mod"
  val STORAGE_CLASS = "x-amz-storage-class"
  val VERSION = "x-amz-version-id"
  val AMZN_HEADERS = List(SOURCE_ETAG, SOURCE_MOD, STORAGE_CLASS, VERSION)

  val REPLACE = "REPLACE"
  val RRS = "REDUCED_REDUNDANCY"

  val ALGORITHM = "HmacSHA1"

  /*HttpRequest pimp*/
  class RichHttpRequest(val req: DefaultHttpRequest) {

    def headers(headers: Map[String, String]): DefaultHttpRequest = {
      headers.foreach(h => req.setHeader(h._1, h._2))
      req
    }

    def sign(bucket: String)(implicit s3key: S3Key, s3secret: S3Secret): DefaultHttpRequest = {
      req.setHeader(AUTHORIZATION, authorization(s3key.key, s3secret.secret, req, bucket))
      req
    }

    def s3headers(bucket: String)(implicit s3key: S3Key, s3secret: S3Secret): DefaultHttpRequest = {
      headers(Map(HOST -> bucketHost(bucket), DATE -> amzDate)).sign(bucket)
    }

    /*
    use query after calling sign so that the query is not used in the signing process
    todo phantom types to enforce
    */
    def query(query: Map[String, String]): DefaultHttpRequest = {
      req.setUri(req.getUri + "?" + query.map(qp => (qp._1 + "=" + qp._2)).reduceLeft(_ + "&" + _))
      req
    }

  }

  implicit def reqToRichReq(req: DefaultHttpRequest): RichHttpRequest = new RichHttpRequest(req)

  class RichServiceFactory[Req, Res](val fact: ServiceFactory[Req, Res]) {
    def tryService(req: Req, otherwise: Res, msg:String): Future[Res] = {
      if(fact.isAvailable) fact.service(req)
      else {
        log.warning("service factory for: %s ->unavailable due to failure accrual",msg)
        Future.value(otherwise)
      }
    }

  }
  implicit def factToRichFact[Req,Res](fact: ServiceFactory[Req,Res]): RichServiceFactory[Req,Res] = new RichServiceFactory[Req,Res](fact)


  /*req creation sugar*/
  def get(uri: String) = new DefaultHttpRequest(HTTP_1_1, GET, uri)

  def put(uri: String) = new DefaultHttpRequest(HTTP_1_1, PUT, uri)

  def head(uri: String) = new DefaultHttpRequest(HTTP_1_1, HEAD, uri)

  def delete(uri: String) = new DefaultHttpRequest(HTTP_1_1, DELETE, uri)

  def ok() = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK)

  /*header utils*/
  def bucketHost(bucket: String) = bucket + ".s3.amazonaws.com"

  def amzDate: String = format.print(new DateTime)

  /*request signing for amazon*/
  /*Create the Authorization payload and sign it with the AWS secret*/
  def sign(secret: String, request: HttpRequest, bucket: String): String = {
    val data = List(
      request.getMethod.getName,
      Option(request.getHeader(CONTENT_MD5)).getOrElse(""),
      Option(request.getHeader(CONTENT_TYPE)).getOrElse(""),
      request.getHeader(DATE)
    ).foldLeft("")(_ + _ + "\n") + normalizeAmzHeaders(request) + "/" + bucket + request.getUri
    log.debug(data)
    calculateHMAC(secret, data)
  }

  def normalizeAmzHeaders(request: HttpRequest): String = {
    AMZN_HEADERS.foldLeft("") {
      (str, h) => {
        Option(request.getHeader(h)).flatMap(v => Some(str + h + ":" + v + "\n")).getOrElse(str)
      }
    }

  }

  def authorization(s3key: String, s3Secret: String, req: HttpRequest, bucket: String): String = {
    "AWS " + s3key + ":" + sign(s3Secret, req, bucket)
  }

  /*Sign the authorization payload*/
  private def calculateHMAC(key: String, data: String): String = {
    val signingKey = new SecretKeySpec(key.getBytes("UTF-8"), ALGORITHM)
    val mac = Mac.getInstance(ALGORITHM)
    mac.init(signingKey)
    val rawHmac = mac.doFinal(data.getBytes())
    new sun.misc.BASE64Encoder().encode(rawHmac)
  }
}
