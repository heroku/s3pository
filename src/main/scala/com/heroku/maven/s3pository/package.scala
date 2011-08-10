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
import com.twitter.util.Future
import com.twitter.finagle.{CancelledConnectionException, WriteException, CancelledRequestException, Service}

package object s3pository {

  lazy val log = Logger.get("s3pository")

  /*DateTime format required by AWS*/
  lazy val amzFormat = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z").withLocale(java.util.Locale.US).withZone(DateTimeZone.forOffsetHours(0))

  /*headers used by this app that need to be used in signing*/
  val SOURCE_ETAG = "x-amz-meta-source-etag"
  val SOURCE_MOD = "x-amz-meta-source-mod"
  val COPY_SOURCE = "x-amz-copy-source"
  val ACL = "x-amz-acl"
  val STORAGE_CLASS = "x-amz-storage-class"
  val VERSION = "x-amz-version-id"
  //headers need to be in alphabetical order in this list
  val AMZN_HEADERS = List(ACL, COPY_SOURCE, SOURCE_ETAG, SOURCE_MOD, STORAGE_CLASS, VERSION)

  val REPLACE = "REPLACE"
  val RRS = "REDUCED_REDUNDANCY"

  val ALGORITHM = "HmacSHA1"

  class RichHttpRequest(val req: HttpRequest) {

    def headers(headers: (String, String)*): HttpRequest = {
      headers.foreach(h => req.setHeader(h._1, h._2))
      req
    }

    def sign(bucket: String)(implicit s3key: S3Key, s3secret: S3Secret): HttpRequest = {
      req.setHeader(AUTHORIZATION, authorization(s3key.key, s3secret.secret, req, bucket))
      req
    }

    def s3headers(bucket: String)(implicit s3key: S3Key, s3secret: S3Secret): HttpRequest = {
      headers(HOST -> bucketHost(bucket), DATE -> amzDate).sign(bucket)
    }

    def query(query: (String,String)*): HttpRequest = {
      req.setUri(req.getUri + "?" + query.map(qp => (qp._1 + "=" + qp._2)).reduceLeft(_ + "&" + _))
      req
    }

    def header(name: String): Option[String] = {
      Option(req.getHeader(name))
    }

    def ifHeader(name: String)(f: String => Unit) {
      req.header(name).foreach(f(_))
    }

  }

  implicit def reqToRichReq(req: HttpRequest): RichHttpRequest = new RichHttpRequest(req)

  class RichHttpResponse(val resp: HttpResponse) {

    def header(name: String): Option[String] = {
      Option(resp.getHeader(name))
    }

    def ifHeader(name: String)(f: String => Unit) {
      resp.header(name).foreach(f(_))
    }

    def hasContent:Boolean = ! hasNoContent

    def hasNoContent:Boolean = {
      "0".equals(resp.getHeader(CONTENT_LENGTH))
    }

  }

  implicit def respToRichResp(resp: HttpResponse): RichHttpResponse = new RichHttpResponse(resp)


  class RichService[Req, Res](val service: Service[Req, Res]) {
    def tryService(req: Req, otherwise: Res, id: String)(msg: String, items: Any*): Future[Res] = {
      service(req).handle {
        case eex: CancelledRequestException => {
          log.debug("Recieved an expected exception type, nothing to see here")
          log.debug(eex, id + " " + msg, items: _*)
          otherwise
        }
        case wex: WriteException => {
          wex.toString() match {
            //Workaround to the fa t that WriteException dosent expose the casue except in toString
            case x if (x.endsWith(classOf[CancelledRequestException].getSimpleName)) => {
              log.debug("Recieved an expected exception type, nothing to see here")
              log.debug(wex, id + " " + msg, items: _*)
              otherwise
            }
            case x if (x.endsWith(classOf[CancelledConnectionException].getSimpleName)) => {
              log.debug("Recieved an expected exception type, nothing to see here")
              log.debug(wex, id + " " + msg, items: _*)
              otherwise
            }
            case _ => {
              log.warning(id + " " + msg + ":" + wex.toString, items: _*)
              log.debug(wex, id + " " + msg, items: _*)
              otherwise
            }
          }
        }
        case ex@_ => {
          log.warning(id + " " + msg + ":" + ex.getClass.getSimpleName, items: _*)
          log.debug(ex, id + " " + msg, items: _*)
          otherwise
        }
      }
    }

  }

  implicit def serviceToRichService[Req, Res](s: Service[Req, Res]): RichService[Req, Res] = new RichService[Req, Res](s)


  /*req creation sugar*/
  def get(uri: String) = new DefaultHttpRequest(HTTP_1_1, GET, uri)

  def put(uri: String) = new DefaultHttpRequest(HTTP_1_1, PUT, uri)

  def head(uri: String) = new DefaultHttpRequest(HTTP_1_1, HEAD, uri)

  def delete(uri: String) = new DefaultHttpRequest(HTTP_1_1, DELETE, uri)

  def ok() = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK)

  /*header utils*/
  def bucketHost(bucket: String) = bucket + ".s3.amazonaws.com"

  def amzDate: String = amzFormat.print(new DateTime)

  /*request signing for amazon*/
  /*Create the Authorization payload and sign it with the AWS secret*/
  def sign(secret: String, request: HttpRequest, bucket: String): String = {
    val data = List(
      request.getMethod.getName,
      request.header(CONTENT_MD5).getOrElse(""),
      request.header(CONTENT_TYPE).getOrElse(""),
      request.getHeader(DATE)
    ).foldLeft("")(_ + _ + "\n") + normalizeAmzHeaders(request) + "/" + bucket + request.getUri
    log.debug(data)
    calculateHMAC(secret, data)
  }

  def normalizeAmzHeaders(request: HttpRequest): String = {
    AMZN_HEADERS.foldLeft("") {
      (str, h) => {
        request.header(h).flatMap(v => Some(str + h + ":" + v + "\n")).getOrElse(str)
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
