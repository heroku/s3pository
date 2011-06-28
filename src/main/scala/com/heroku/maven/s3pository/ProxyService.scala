package com.heroku.maven.s3pository

import com.twitter.finagle.http.Http
import com.twitter.finagle.Service

import java.net.InetSocketAddress
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.conversions.time._
import collection.immutable.HashMap
import collection.mutable.{HashMap => MMap}
import org.jboss.netty.buffer.ChannelBuffer
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import javax.crypto.spec.SecretKeySpec
import javax.crypto.Mac
import java.util.logging.Logger
import collection.JavaConversions._
import org.jboss.netty.handler.codec.http._
import com.twitter.util._

case class ProxiedRepository(prefix: String, host: String, hostPath: String, bucket: String)

case class HitTracker(client: Client, future: Future[HttpResponse])

case class RepositoryGroup(prefix: String, repos: List[ProxiedRepository]) {
  val hits = new MMap[String, ProxiedRepository]
}

case class Client(repoService: Service[HttpRequest, HttpResponse], s3Service: Service[HttpRequest, HttpResponse], repo: ProxiedRepository)

class ProxyService(repositories: List[ProxiedRepository], groups: List[RepositoryGroup], s3key: String, s3Secret: String) extends Service[HttpRequest, HttpResponse] {

  import ProxyService._

  val log = Logger.getLogger(getClass.getName)

  val timer = new JavaTimer(true)

  val clients: HashMap[String, Client] = {
    repositories.foldLeft(new HashMap[String, Client]) {
      (m, repo) => {
        m + (repo.prefix -> Client(clientService(repo.host), clientService(repo.bucket + ".s3.amazonaws.com"), repo))
      }
    }
  }

  val repositoryGroups: HashMap[String, RepositoryGroup] = {
    groups.foldLeft(new HashMap[String, RepositoryGroup]) {
      (m, g) => m + (g.prefix -> g)
    }
  }

  def clientService(host: String): Service[HttpRequest, HttpResponse] = {
    import com.twitter.conversions.storage._
    ClientBuilder()
      .codec(Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabyte))
      .sendBufferSize(1048576)
      .recvBufferSize(1048576)
      .hosts(new InetSocketAddress(host, 80))
      .hostConnectionLimit(10)
      .logger(Logger.getLogger("client"))
      .build()
  }


  def apply(request: HttpRequest) = {
    val prefix = getPrefix(request)
    val contentUri = getContentUri(prefix, request.getUri)
    repositoryGroups.get(prefix) match {
      /*request matches a group*/
      case Some(group) => {
        group.hits.get(contentUri) match {
          /*group has a hit for the contentUri so go directly to the right proxy*/
          case Some(proxiedRepo) => {
            log.info("%s cache hit on %s".format(contentUri, proxiedRepo.host))
            singleRepo(clients.get(proxiedRepo.prefix).get, contentUri, request)
          }
          /*group dosent have a hit, iterate through and try and find the content in a proxy*/
          case None => {

            val trackers = group.repos.map {
              repo => {
                val client = clients.get(repo.prefix).get
                val future = singleRepo(client, contentUri, cloneRequest(request)).within(timer,10.seconds) handle {
                  case _: TimeoutException => timeout
                }
                HitTracker(client, future)
              }
            }

            var finalResponse: Future[HttpResponse] = null

            trackers.foreach {
              tracker => {
                val response = tracker.future.get()
                response.getStatus.getCode match {
                  case code if (code == 200 && finalResponse == null) => {
                    group.hits += (contentUri -> tracker.client.repo)
                    finalResponse = new Promise[HttpResponse](Return(response))
                    ()
                  }
                  case _ => ()
                }
              }
            }

            finalResponse

          }
        }
      }
      case None => {
        clients.get(prefix) match {
          /*request matches a single proxied repo*/
          case Some(client) => singleRepo(client, contentUri, request)
          /*no match*/
          case None => Future.value(notFound)
        }
      }
    }
  }


  def singleRepo(client: Client, content: String, request: HttpRequest): Future[HttpResponse] = {
    val s3request: DefaultHttpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, content)
    s3request.setHeader("Host", client.repo.bucket + ".s3.amazonaws.com")
    s3request.setHeader("Date", date)
    s3request.setHeader("Authorization", "AWS " + s3key + ":" + sign(s3Secret, s3request, client.repo.bucket))
    client.s3Service(s3request).flatMap {
      s3response => {
        s3response.getStatus.getCode match {
          case code if (code == 200) => {
            log.info("SERVING FROM S3:" + content)
            new Promise[HttpResponse](Return(s3response))
          }
          case code if (code == 404) => {
            val uri = client.repo.hostPath + content
            request.setUri(uri)
            request.setHeader("Host", client.repo.host)
            val responseFuture = client.repoService(request)
            responseFuture.flatMap {
              response => {
                if (response.getStatus == HttpResponseStatus.OK) {
                  val s3buffer = response.getContent.duplicate()
                  putS3(client, request, content, response.getHeader("Content-Type"), s3buffer)
                } else {
                  log.warning("Request to Source repo %s: path: %s Status Code: %s".format(client.repo.host, request.getUri, response.getStatus.getReasonPhrase))
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
    client.s3Service {
      s3Put
    } onSuccess {
      resp => log.info("S3Put Success: Code %s, Content %s ".format(resp.getStatus.getReasonPhrase, resp.getContent.toString("UTF-8")))
    } onFailure {
      ex => log.severe("Exception in S3 Put: " + ex.getStackTraceString)
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
    "/" + uri.substring(0, uri.indexOf("/"))
  }

  def getContentUri(prefix: String, source: String): String = {
    source.substring(source.indexOf(prefix) + prefix.length())
  }
}

object ProxyService {
  lazy val format = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z").withLocale(java.util.Locale.US).withZone(DateTimeZone.forOffsetHours(0))

  def date: String = format.print(new DateTime)

  val ALGORITHM = "HmacSHA1"

  def notFound = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)

  def timeout = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.GATEWAY_TIMEOUT)

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






