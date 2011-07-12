package com.heroku.maven.s3pository

import com.heroku.maven.s3pository.S3rver._

import com.twitter.logging.Logger
import com.twitter.logging.config.{ConsoleHandlerConfig, LoggerConfig}
import com.twitter.util.Future
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.Service

import java.net.InetSocketAddress

import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.handler.codec.http._

import util.Properties

import xml.XML



/*checks for updated artifacts in source repos*/
object S3Updater {

  lazy val log = Logger.get("S3Server-Updater")

  def main(args: Array[String]) {
    Logger.clearHandlers()
    val logConf = new LoggerConfig {
      node = ""
      level = Logger.levelNames.get(Properties.envOrElse("LOG_LEVEL", "INFO"))
      handlers = List(new ConsoleHandlerConfig, new NewRelicLogHandlerConfig)
    }
    logConf.apply()
    val supressNettyWarning = new LoggerConfig {
      node = "org.jboss.netty.channel.SimpleChannelHandler"
      level = Logger.ERROR
    }
    supressNettyWarning.apply()
    log.warning("Starting Updater")

    val s3Client = client("s3.amazonaws.com")

    proxies foreach {
      proxy: ProxiedRepository => {
        val sourceClient = client(proxy)

        val keys = getKeys(s3Client, proxy.bucket)

        val futures: Seq[Future[HttpResponse]] = keys map {
          key => {
            /*get the orig last modified and or etag from s3, either or both can be null*/
            val metaReq = head("/" + key).s3headers(s3key, s3secret, proxy.bucket)
            log.debug("checking %s for %s", proxy.bucket, key)
            val future = s3Client(metaReq).onFailure(log.error(_, "error getting s3 metadata for %s in %s", key, proxy.bucket))
            future flatMap {
              metaResp => {
                if ((metaResp.getHeader(SOURCE_ETAG) ne null) || (metaResp.getHeader(SOURCE_MOD) ne null)) {
                  /*s3 had a source etag or last mod*/
                  /*do a head on the origin repo and compare*/
                  val sourceReq = head(proxy.hostPath + "/" + key).headers(Map(HOST -> proxy.host))
                  sourceClient(sourceReq).onFailure(log.error(_, "error checking source %s for %s", proxy.host, sourceReq.getUri)).flatMap {
                    sourceResp => {
                      if (sourceResp.getStatus.getCode == 200) {
                        /*we compare etag first*/
                        if (metaResp.getHeader(SOURCE_ETAG) ne null) {
                          if (!metaResp.getHeader(SOURCE_ETAG).equals(sourceResp.getHeader(ETAG))) {
                            log.warning("etag for %s changed, updating in S3", sourceReq.getUri)
                            sourceReq.setMethod(HttpMethod.GET)
                            updateS3(sourceClient, s3Client, proxy.bucket, "/" + key, sourceReq)
                          } else {
                            log.debug("etag for %s unchanged", sourceReq.getUri)
                            Future.value(ok())
                          }
                        } else {
                          /*otherwise chech mod date*/
                          if (!metaResp.getHeader(SOURCE_MOD).equals(sourceResp.getHeader(LAST_MODIFIED))) {
                            log.warning("last changed date for %s changed, updating in S3", sourceReq.getUri)
                            updateS3(sourceClient, s3Client, proxy.bucket, "/" + key, sourceReq)
                          } else {
                            log.debug("last changed date for %s unchanged", sourceReq.getUri)
                            Future.value(ok())
                          }
                        }
                      } else {
                        log.warning("attempetd to get %s from %s, code %s", sourceReq.getUri, proxy.host, sourceResp.getStatus.getCode.toString)
                        Future.value(ok())
                      }
                    }
                  }
                } else {
                  log.debug("no etag or mod date for %s", key)
                  Future.value(ok())
                }
              }
            }
          }
        }

        /*wait for responses*/
        Future.collect(futures).get().foreach {
          resp => {
            if (resp.getStatus.getCode == 200) {
              log.debug("S3Updater Success: Code %s, Content %s ", resp.getStatus.getReasonPhrase, resp.getContent.toString("UTF-8"))
            } else {
              log.error(new RuntimeException(resp.getContent.toString("UTF-8")), "S3Put did not return a 200")
            }
          }
        }
        sourceClient.release()


      }
    }
    s3Client.release()

  }

  /*get the keys in an s3bucket, s3 only returns up to 1000 at a time so this can be called recursively*/
  def getKeys(client: Service[HttpRequest, HttpResponse], bucket: String, marker: Option[String] = None): List[String] = {
    val listRequest = get("/").s3headers(s3key, s3secret, bucket)
    marker.foreach(m => listRequest.query(Map("marker" -> m)))
    val listResp = client(listRequest).onFailure(log.error(_, "error getting keys for bucket %s marker %s", bucket, marker)).get()
    val respStr = listResp.getContent.toString("UTF-8")
    log.debug(respStr)
    val xResp = XML.loadString(respStr)

    val keys = (xResp \\ "Contents" \\ "Key") map (_.text) toList
    val truncated = ((xResp \ "IsTruncated") map (_.text.toBoolean))
    log.warning("Got %s keys for %s", keys.size.toString, bucket)
    if (truncated.head) {
      keys ++ getKeys(client, bucket, Some(keys.last))
    } else {
      keys
    }
  }

  /*do a get for the updated content, delete the existing s3 item and pipeline the get to a put of the updated content*/
  def updateS3(sourceClient: Service[HttpRequest, HttpResponse], s3Client: Service[HttpRequest, HttpResponse], bucket: String, contentUri: String, req: DefaultHttpRequest): Future[HttpResponse] = {
    req.setMethod(HttpMethod.GET)
    sourceClient(req).onFailure(log.error(_, "error on GET %s to update S3 bucket %s", req.getUri, bucket)).flatMap {
      response =>
        val s3del = delete(contentUri).s3headers(s3key, s3secret, bucket)
        s3Client(s3del).onFailure(log.error(_, "error on DEL %s to update S3 bucket %s", s3del.getUri, bucket)).flatMap {
          delResp => {
            val s3Put = put(contentUri).headers(Map(CONTENT_LENGTH -> response.getContent.readableBytes.toString,
              CONTENT_TYPE -> response.getHeader(CONTENT_TYPE),
              STORAGE_CLASS -> RRS,
              HOST -> bucketHost(bucket),
              DATE -> amzDate))
            Option(response.getHeader(ETAG)).foreach(s3Put.setHeader(SOURCE_ETAG, _))
            Option(response.getHeader(LAST_MODIFIED)).foreach(s3Put.setHeader(SOURCE_MOD, _))
            s3Put.setContent(response.getContent)
            s3Put.sign(s3key, s3secret, bucket)
            s3Client(s3Put).onFailure(log.error(_, "error on  PUT %s to update S3 bucket %s", req.getUri, bucket))
          }
        }

    }
  }

  def client(repo: ProxiedRepository): Service[HttpRequest, HttpResponse] = {
    client(repo.host, repo.port, repo.ssl)
  }

  def client(host: String, port: Int = 80, ssl: Boolean = false): Service[HttpRequest, HttpResponse] = {
    var builder = ClientBuilder()
      .codec(Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabyte))
      .sendBufferSize(1048576)
      .recvBufferSize(1048576)
      .hosts(new InetSocketAddress(host, port))
      .hostConnectionLimit(8)
      .keepAlive(true)
      .hostConnectionMaxWaiters(Integer.MAX_VALUE)
      .requestTimeout(30.seconds)
      .connectionTimeout(5.seconds)
      .name(host)
    if (ssl) (builder = builder.tlsWithoutValidation())
    builder.build()
  }
}