package com.heroku

import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._


import com.twitter.logging.Logger
import org.jboss.netty.handler.codec.http._
import com.twitter.util.Future
import com.twitter.finagle.{CancelledConnectionException, WriteException, CancelledRequestException, Service}

package object s3pository {

  lazy val log = Logger.get("s3pository")


  class RichHttpRequest(val req: HttpRequest) {

    def headers(headers: (String, String)*): HttpRequest = {
      headers.foreach(h => req.setHeader(h._1, h._2))
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

    def hasContent: Boolean = !hasNoContent

    def hasNoContent: Boolean = {
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

}
