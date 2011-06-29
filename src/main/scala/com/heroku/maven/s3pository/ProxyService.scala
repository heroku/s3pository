package com.heroku.maven.s3pository

import com.twitter.finagle.http.Http
import java.net.InetSocketAddress
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.conversions.time._
import collection.immutable.HashMap
import collection.mutable.{HashMap => MMap}
import org.joda.time.format.DateTimeFormat
import javax.crypto.spec.SecretKeySpec
import javax.crypto.Mac
import collection.JavaConversions._
import com.twitter.util._
import org.jboss.netty.handler.codec.http._
import org.joda.time.{DateTime, DateTimeZone}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import java.util.logging.{Level, Logger}
import com.twitter.finagle.{ServiceFactory, Service}

case class ProxiedRepository(prefix: String, host: String, hostPath: String, bucket: String)

case class HitTracker(client: Client, future: Future[HttpResponse])

case class RepositoryGroup(prefix: String, repos: List[ProxiedRepository]) {
  val hits = new MMap[String, ProxiedRepository]
  val misses = new MMap[String, DateTime]
}

case class Client(repoService: ServiceFactory[HttpRequest, HttpResponse], s3Service: ServiceFactory[HttpRequest, HttpResponse], repo: ProxiedRepository)

class ProxyService(repositories: List[ProxiedRepository], groups: List[RepositoryGroup], s3key: String, s3Secret: String) extends Service[HttpRequest, HttpResponse] {

  import ProxyService._

  val log = Logger.getLogger(getClass.getName)
  log.info("creating ProxyService")

  val timer = new JavaTimer(true)

  val clients: HashMap[String, Client] = {
    repositories.foldLeft(new HashMap[String, Client]) {
      (m, repo) => {
        m + (repo.prefix -> Client(clientService(repo.host), clientService(repo.bucket + ".s3.amazonaws.com"), repo))
      }
    }
  }

  clients.values.foreach(createBucket(_))
  log.info("S3 Buckets verified")

  val repositoryGroups: HashMap[String, RepositoryGroup] = {
    groups.foldLeft(new HashMap[String, RepositoryGroup]) {
      (m, g) => m + (g.prefix -> g)
    }
  }

  def clientService(host: String): ServiceFactory[HttpRequest, HttpResponse] = {
    import com.twitter.conversions.storage._
    ClientBuilder()
      .codec(Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabyte))
      .sendBufferSize(1048576)
      .recvBufferSize(1048576)
      .hosts(new InetSocketAddress(host, 80))
      .hostConnectionLimit(Integer.MAX_VALUE)
      .hostConnectionMaxIdleTime(5.seconds)
      .logger(Logger.getLogger("finagle.client"))
      .buildFactory()
  }

  def createBucket(client: Client) {
    log.fine("creating bucket: %s".format(client.repo.bucket))
    val s3request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "/")
    s3request.setHeader("Host", client.repo.bucket + ".s3.amazonaws.com")
    s3request.setHeader("Date", date)
    s3request.setHeader("Content-Length", "0")
    s3request.setHeader("Authorization", "AWS " + s3key + ":" + sign(s3Secret, s3request, client.repo.bucket))
    client.s3Service.service(s3request) onSuccess {
      response =>
        if (response.getStatus.getCode != 200) {
          log.info("Create Bucket %s return code %d".format(client.repo.bucket, response.getStatus.getCode))
          log.info(response.getContent.toString("UTF-8"))
        } else {
          log.info("Create Bucket %s return code %d".format(client.repo.bucket, response.getStatus.getCode))
        }
    } onFailure {
      ex =>
        log.log(Level.SEVERE, "failure while creating bucket:%s".format(client.repo.bucket), ex)
    } onCancellation {
      log.warning("create bucket: %s was cancelled".format(client.repo.bucket))
    }
  }


  def apply(request: HttpRequest) = {
    log.info("Request for: %s".format(request.getUri))
    val prefix = getPrefix(request)
    val contentUri = getContentUri(prefix, request.getUri)
    repositoryGroups.get(prefix) match {
      /*request matches a group*/
      case Some(group) => {
        log.info("Group request: %s".format(group.prefix))
        groupRepoRequest(group, contentUri, request)
      }
      case None => {
        clients.get(prefix) match {
          /*request matches a single proxied repo*/
          case Some(client) => {
            log.info("Single repo request: %s".format(prefix))
            singleRepoRequest(client, contentUri, request)
          }
          /*no match*/
          case None => {
            log.info("Unknown prefix: %s".format(prefix))
            Future.value(notFound)
          }
        }
      }
    }
  }

  def groupRepoRequest(group: RepositoryGroup, contentUri: String, request: HttpRequest): Future[HttpResponse] = {
    group.hits.get(contentUri) match {
      /*group has a hit for the contentUri so go directly to the right proxy*/
      case Some(proxiedRepo) => {
        log.info("%s cache hit on %s".format(contentUri, proxiedRepo.host))
        singleRepoRequest(clients.get(proxiedRepo.prefix).get, contentUri, request)
      }
      /*group dosent have a hit, iterate through and try and find the contentUri in a proxy*/
      case None => {
        group.misses.get(contentUri) match {
          case None => groupParallelRequest(group, contentUri, request)
          case Some(time) if (time.plusMinutes(30).isBeforeNow) => {
            log.info("invalidating cached miss for %s".format(contentUri))
            group.misses.remove(contentUri)
            groupParallelRequest(group, contentUri, request)
          }
          case _ => {
            log.info("returning 404, cached miss for %s".format(contentUri))
            new Promise[HttpResponse](Return(notFound))
          }
        }
      }
    }
  }

  def groupParallelRequest(group: RepositoryGroup, contentUri: String, request: HttpRequest): Future[HttpResponse] = {
    val trackers = group.repos.map {
      repo => {
        val client = clients.get(repo.prefix).get
        log.info("parallel request for %s to %s".format(contentUri, repo.host))
        val future = singleRepoRequest(client, contentUri, cloneRequest(request)).within(timer, 10.seconds) handle {
          case _: TimeoutException => timeout
        }
        HitTracker(client, future)
      }
    }

    new Promise[HttpResponse](Return(firstAcceptableResponse(trackers)(group, contentUri)))

  }

  def firstAcceptableResponse(trackers: List[HitTracker])(implicit group: RepositoryGroup, contentUri: String): HttpResponse = {
    trackers.headOption match {
      case Some(tracker) => {
        val response = {
          try {
            tracker.future.get()
          } catch {
            case _ => notFound
          }
        }
        if (response.getStatus.getCode == 200) {
          group.hits += (contentUri -> tracker.client.repo)
          trackers.tail.foreach(_.future.cancel())
          response
        } else {
          firstAcceptableResponse(trackers.tail)
        }
      }
      case None => notFound
    }
  }

  def singleRepoRequest(client: Client, contentUri: String, request: HttpRequest): Future[HttpResponse] = {
    val s3request: DefaultHttpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, contentUri)
    s3request.setHeader("Host", client.repo.bucket + ".s3.amazonaws.com")
    s3request.setHeader("Date", date)
    s3request.setHeader("Authorization", "AWS " + s3key + ":" + sign(s3Secret, s3request, client.repo.bucket))
    client.s3Service.service(s3request).flatMap {
      s3response => {
        s3response.getStatus.getCode match {
          case code if (code == 200) => {
            log.info("Serving from S3 bucket %s: %s".format(client.repo.bucket, contentUri))
            new Promise[HttpResponse](Return(s3response))
          }
          case code if (code == 404) => {
            val uri = client.repo.hostPath + contentUri
            request.setUri(uri)
            request.setHeader("Host", client.repo.host)
            val responseFuture = client.repoService.service(request).onFailure {
              ex =>
                if (!ex.isInstanceOf[Future.CancelledException]) {
                  log.log(Level.SEVERE, "request to %s for %s threw %s, returning 404".format(client.repo.host, request.getUri, ex.getClass.getSimpleName), ex)
                }
                new Promise[HttpResponse](Return(notFound))
            }
            responseFuture.flatMap {
              response => {
                if (response.getStatus == HttpResponseStatus.OK) {
                  log.info("Serving from Source %s: %s".format(client.repo.host, contentUri))
                  val s3buffer = response.getContent.duplicate()
                  putS3(client, request, contentUri, response.getHeader("Content-Type"), s3buffer)
                } else {
                  log.warning("Request to Source repo %s: path: %s Status Code: %s".format(client.repo.host, request.getUri, response.getStatus.getCode))
                }
                new Promise[HttpResponse](Return(response))
              }
            }
          }
          case code@_ => {
            log.severe("Recieved code:" + code)
            log.severe(s3response.getContent.toString("UTF-8"))
            Future.value(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR))
          }
        }
      }
    }
  }

  def putS3(client: Client, request: HttpRequest, contentUri: String, contentType: String, content: ChannelBuffer) {
    val s3Put = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, contentUri)
    s3Put.setContent(content)
    //seems slow and barfs in the log but eventually succeeds. Revisit.
    //s3Put.setHeader("Expect","100-continue")
    s3Put.setHeader("Content-Length", content.readableBytes)
    s3Put.setHeader("Content-Type", contentType)
    s3Put.setHeader("Host", client.repo.bucket + ".s3.amazonaws.com")
    s3Put.setHeader("Date", date)
    s3Put.setHeader("Authorization", "AWS " + s3key + ":" + sign(s3Secret, s3Put, client.repo.bucket))
    client.s3Service.service {
      s3Put
    } onSuccess {
      resp => log.info("S3Put Success: Code %s, Content %s ".format(resp.getStatus.getReasonPhrase, resp.getContent.toString("UTF-8")))
    } onFailure {
      ex => log.log(Level.SEVERE, "Exception in S3 Put: ", ex)
    } onCancellation {
      log.severe("S3Put cancelled" + s3Put.toString)
    }
  }

  def sign(secret: String, request: HttpRequest, bucket: String): String = {
    val data = List(
      request.getMethod.getName,
      Option(request.getHeader("Content-MD5")).getOrElse(""),
      Option(request.getHeader("Content-Type")).getOrElse(""),
      request.getHeader("Date")
    ).foldLeft("")(_ + _ + "\n") + "/" + bucket + request.getUri
    calculateHMAC(secret, data)
  }

  private def calculateHMAC(key: String, data: String): String = {
    val signingKey = new SecretKeySpec(key.getBytes("UTF-8"), ALGORITHM)
    val mac = Mac.getInstance(ALGORITHM)
    mac.init(signingKey)
    val rawHmac = mac.doFinal(data.getBytes())
    new sun.misc.BASE64Encoder().encode(rawHmac)
  }


  def getPrefix(request: HttpRequest): String = {
    val uri = request.getUri.substring(1)
    val index = uri.indexOf("/")
    if (index != -1) {
      "/" + uri.substring(0, index)
    } else {
      "unknown prefix"
    }
  }

  def getContentUri(prefix: String, source: String): String = {
    if (source.contains(prefix)) {
      source.substring(source.indexOf(prefix) + prefix.length())
    } else {
      source
    }
  }
}

object ProxyService {
  lazy val format = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z").withLocale(java.util.Locale.US).withZone(DateTimeZone.forOffsetHours(0))

  def date: String = format.print(new DateTime)

  val ALGORITHM = "HmacSHA1"

  def notFound = {
    val resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)
    resp.setContent(ChannelBuffers.wrappedBuffer(
      """
      <html>
      <head><title>404 Not Found</title></head>
      <body>
      <h2>404 Not Found</h2>
      </body>
      </html>
      """.getBytes
    ))
    resp
  }

  def timeout = {
    val resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.GATEWAY_TIMEOUT)
    resp.setContent(ChannelBuffers.wrappedBuffer(
      """
      <html>
      <head><title>504 GatewayTimeout</title></head>
      <body>
      <h2>504 Gateway Timeout</h2>
      </body>
      </html>
      """.getBytes
    ))
    resp
  }

  def cloneRequest(request: HttpRequest): HttpRequest = {
    val cloned = new DefaultHttpRequest(request.getProtocolVersion, request.getMethod, request.getUri)
    cloned.setChunked(request.isChunked)
    cloned.setContent(request.getContent.duplicate())
    request.getHeaderNames.iterator.foreach {
      header =>
        cloned.setHeader(header, request.getHeader(header))
    }
    cloned
  }
}






