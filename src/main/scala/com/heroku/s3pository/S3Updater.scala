package com.heroku.s3pository

import com.heroku.s3pository.S3rver._
import com.heroku.s3pository.ProxyService._

import com.twitter.logging.Logger
import com.twitter.finagle.Service
import com.twitter.util.{Time, Future}


import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.handler.codec.http._
import com.heroku.finagle.aws._
import com.twitter.finagle.stats.{NullStatsReceiver, SummarizingStatsReceiver}


/*checks for updated artifacts in source repos*/
object S3Updater {

  type Client = Service[HttpRequest, HttpResponse]
  lazy val log = Logger.get("S3Server-Updater")
  lazy val stats = new SummarizingStatsReceiver

  def main(args: Array[String]) {
    configureLogging()
    log.warning("Starting Updater")
    val s3Client = s3Service(s3key, s3secret, "updater", new NullStatsReceiver)
    val start = Time.now
    proxies foreach {
      proxy: ProxiedRepository => {
        val sourceClient = client(proxy)
        val keys = ListBucket.getKeys(s3Client, proxy.bucket)
        stats.counter("bucket", proxy.bucket, "totalkeys").incr(keys.size)
        //do in batches of 100 to keep queue depths and memory consumption down
        if (args.size > 0) log.info("filtering keys with " + args.mkString(" | "))
        keys.filter(args.size == 0 || contains(_, args.toList)).grouped(100) foreach {
          keygroup => {
            stats.counter("bucket", proxy.bucket, "filteredkeys").incr(keygroup.size)
            doUpdate(s3Client, sourceClient, proxy, keygroup)
          }
        }
        sourceClient.release()
      }
    }

    s3Client.release()
    val end = Time.now - start
    log.warning("total time (seconds) %d", end.inSeconds)
    log.warning(stats.summary)
    System.exit(0)
  }

  def contains(key: String, filters: List[String]): Boolean = {
    filters.headOption match {
      case None => false
      case Some(filter) => {
        key.contains(filter) || contains(key, filters.tail)
      }
    }
  }

  def doUpdate(s3Client: S3.S3Client, sourceClient: Client, proxy: ProxiedRepository, keys: List[String]) {
    val futures: Seq[Future[HttpResponse]] = keys map {
      key => {
        /*get the orig last modified and or etag from s3, either or both can be null*/
        log.debug("checking %s for %s", proxy.bucket, key)
        val future = s3Client(Head(proxy.bucket, "/" + key)).onFailure(log.error(_, "error getting s3 metadata for %s in %s", key, proxy.bucket))
        future flatMap {
          metaResp => {
            if ((metaResp.getHeader(SOURCE_ETAG) ne null) || (metaResp.getHeader(SOURCE_MOD) ne null)) {
              /*s3 had a source etag or last mod*/
              /*do a head on the origin repo and compare*/
              val sourceReq = head(proxy.hostPath + "/" + key).headers(HOST -> proxy.host)
              sourceClient(sourceReq).onFailure(log.error(_, "error checking source %s for %s", proxy.host, sourceReq.getUri)).flatMap {
                sourceResp => {
                  if (sourceResp.getStatus.getCode == 200) {
                    /*we compare etag first*/
                    if (metaResp.getHeader(SOURCE_ETAG) ne null) {
                      if (!metaResp.getHeader(SOURCE_ETAG).equals(sourceResp.getHeader(ETAG))) {
                        stats.counter("bucket", proxy.bucket, "etag changed").incr(1)
                        log.warning("etag for %s changed, updating in S3", sourceReq.getUri)
                        sourceReq.setMethod(HttpMethod.GET)
                        updateS3(sourceClient, s3Client, proxy.bucket, "/" + key, sourceReq)
                      } else {
                        stats.counter("bucket", proxy.bucket, "etag matched").incr(1)
                        log.debug("etag for %s unchanged", sourceReq.getUri)
                        Future.value(ok())
                      }
                    } else {
                      /*otherwise chech mod date*/
                      if (!metaResp.getHeader(SOURCE_MOD).equals(sourceResp.getHeader(LAST_MODIFIED))) {
                        stats.counter("bucket", proxy.bucket, "mod changed").incr(1)
                        log.warning("last changed date for %s changed, updating in S3", sourceReq.getUri)
                        updateS3(sourceClient, s3Client, proxy.bucket, "/" + key, sourceReq)
                      } else {
                        log.debug("last changed date for %s unchanged", sourceReq.getUri)
                        stats.counter(proxy.bucket, "mod matched").incr(1)
                        Future.value(ok())
                      }
                    }
                  } else {
                    stats.counter("bucket", proxy.bucket, "non 200").incr(1)
                    log.warning("attempetd to get %s from %s, code %s", sourceReq.getUri, proxy.host, sourceResp.getStatus.getCode.toString)
                    Future.value(ok())
                  }
                }
              }
            } else {
              log.debug("no etag or mod date for %s", key)
              stats.counter("bucket", proxy.bucket, "no etag or mod").incr(1)
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
  }


  /*do a get for the updated content, delete the existing s3 item and pipeline the get to a put of the updated content*/
  def updateS3(sourceClient: Service[HttpRequest, HttpResponse], s3Client: Service[S3Request, HttpResponse], bucket: String, contentUri: String, req: HttpRequest): Future[HttpResponse] = {
    req.setMethod(HttpMethod.GET)
    sourceClient(req).onFailure(log.error(_, "error on GET %s to update S3 bucket %s", req.getUri, bucket)).flatMap {
      response =>
        s3Client(Delete(bucket, contentUri)).onFailure(log.error(_, "error on DEL %s to update S3 bucket %s", contentUri, bucket)).flatMap {
          delResp => {
            val s3Put = Put(bucket, contentUri, response.getContent, CONTENT_TYPE -> response.getHeader(CONTENT_TYPE), STORAGE_CLASS -> RRS)
            Option(response.getHeader(ETAG)).foreach(s3Put.setHeader(SOURCE_ETAG, _))
            Option(response.getHeader(LAST_MODIFIED)).foreach(s3Put.setHeader(SOURCE_MOD, _))
            s3Client(s3Put).onFailure(log.error(_, "error on  PUT %s to update S3 bucket %s", req.getUri, bucket))
          }
        }
    }
  }

  def client(repo: ProxiedRepository): Client = {
    clientService(repo.host, repo.port, repo.ssl, "Updater", new NullStatsReceiver)
  }


}