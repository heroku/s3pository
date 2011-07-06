package com.heroku.maven.s3pository

import com.twitter.conversions.time._
import com.twitter.util._
import com.twitter.finagle.{ServiceFactory, Service}
import com.twitter.finagle.http.Http
import com.twitter.finagle.builder.ClientBuilder

import collection.immutable.HashMap
import collection.mutable.{HashMap => MMap}
import collection.JavaConversions._

import java.net.InetSocketAddress
import javax.crypto.spec.SecretKeySpec
import javax.crypto.Mac
import com.twitter.logging.Logger

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import java.lang.IllegalArgumentException
import com.newrelic.api.agent.Trace

/*
HTTP Service that acts as a caching proxy server for the configured ProxiedRepository(s) and RepositoryGroup(s).
An S3 bucket per ProxiedRepositoriy is used to cache the content retrieved from the source repository.
Requests to this service are mapped by requestUri prefix to the ProxyRepository or RepositoryGroup that should be used to service the request.
e.g.
a ProxyService configured with ProxiedRepository("/prefix", "source.repo.com", "/path/to/repo")
will respond to a request for
                  http://0.0.0.0/prefix/some/artifact.ext
by making requests to
    http://source.repo.com/path/to/repo/some/artifact.ext
*/
class ProxyService(repositories: List[ProxiedRepository], groups: List[RepositoryGroup], s3key: String, s3Secret: String) extends Service[HttpRequest, HttpResponse] {

  import ProxyService._

  val log = Logger.get(getClass)
  log.info("creating ProxyService")
  /*Timer used to time box parallel request processing*/
  val timer = new JavaTimer(true)

  val clients: HashMap[String, Client] = {
    repositories.foldLeft(new HashMap[String, Client]) {
      (m, repo) => {
        m + (repo.prefix -> Client(clientService(repo.host, repo.port, repo.ssl), clientService(repo.bucket + ".s3.amazonaws.com", 80, false), repo))
      }
    }
  }
  /*create/verify all S3 buckets at creation time*/
  clients.values.foreach(createBucket(_))
  log.info("S3 Buckets verified")

  val repositoryGroups: HashMap[String, RepositoryGroup] = {
    groups.foldLeft(new HashMap[String, RepositoryGroup]) {
      (m, g) => m + (g.prefix -> g)
    }
  }
  /*Build a Client ServiceFactory for the given endpoint*/
  def clientService(host: String, port: Int, ssl: Boolean): ServiceFactory[HttpRequest, HttpResponse] = {
    import com.twitter.conversions.storage._
    var builder = ClientBuilder()
      .codec(Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabyte))
      .sendBufferSize(1048576)
      .recvBufferSize(1048576)
      .hosts(new InetSocketAddress(host, port))
      .hostConnectionLimit(Integer.MAX_VALUE)
      .hostConnectionMaxIdleTime(5.seconds)
      //.reportTo(NewRelicStatsReceiver)
      .name(host)
    if (ssl) (builder = builder.tlsWithoutValidation())
    builder.buildFactory()
  }

  /*Create any missing S3 buckets. Create bucket is idempotent, and returns a 200 if the bucket exists or is created*/
  def createBucket(client: Client) {
    log.debug("creating bucket: %s".format(client.repo.bucket))
    val s3request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "/")
    s3request.setHeader(HOST, client.repo.bucket + ".s3.amazonaws.com")
    s3request.setHeader(DATE, date)
    s3request.setHeader(CONTENT_LENGTH, "0")
    s3request.setHeader(AUTHORIZATION, "AWS " + s3key + ":" + sign(s3Secret, s3request, client.repo.bucket))
    client.s3Service.service(s3request) onSuccess {
      response =>
        if (response.getStatus.getCode != 200) {
          log.info("Create Bucket %s return code %d", client.repo.bucket, response.getStatus.getCode)
          log.info(response.getContent.toString("UTF-8"))
        } else {
          log.info("Create Bucket %s return code %d", client.repo.bucket, response.getStatus.getCode)
        }
    } onFailure {
      ex =>
        log.error(ex, "failure while creating bucket:%s", client.repo.bucket)
    } onCancellation {
      log.warning("create bucket: %s was cancelled", client.repo.bucket)
    }
  }

  /*main service function for ProxyService, this handles all incoming requests*/
  @Trace
  def apply(request: HttpRequest) = {
    log.info("Request for: %s", request.getUri)
    val prefix = getPrefix(request)
    val contentUri = getContentUri(prefix, request.getUri)
    repositoryGroups.get(prefix) match {
      /*request matches a group*/
      case Some(group) => {
        log.info("Group request: %s", group.prefix)
        groupRepoRequest(group, contentUri, request)
      }
      case None => {
        clients.get(prefix) match {
          /*request matches a single proxied repo*/
          case Some(client) => {
            log.info("Single repo request: %s", prefix)
            singleRepoRequest(client, contentUri, request)
          }
          /*no match*/
          case None => {
            log.info("Unknown prefix: %s", prefix)
            Future.value(notFound)
          }
        }
      }
    }
  }

  @Trace
  def groupRepoRequest(group: RepositoryGroup, contentUri: String, request: HttpRequest): Future[HttpResponse] = {
    group.hits.get(contentUri) match {
      /*group has a hit for the contentUri so go directly to the right proxy*/
      case Some(proxiedRepo) => {
        log.info("%s cache hit on %s", contentUri, proxiedRepo.host)
        singleRepoRequest(clients.get(proxiedRepo.prefix).get, contentUri, request)
      }
      /*group dosent have a hit, try and find the contentUri in one of the groups proxies*/
      case None => {
        group.misses.get(contentUri) match {
          /*no cached misses, do a parallel request to the group proxies*/
          case None => groupParallelRequest(group, contentUri, request)
          /*cached missed is timed out, remove the cache entry and do a parallel request to the group proxies*/
          case Some(time) if (time.plusMinutes(30).isBeforeNow) => {
            log.info("invalidating cached miss for %s", contentUri)
            group.misses.remove(contentUri)
            groupParallelRequest(group, contentUri, request)
          }
          /*we have a valid cached miss, so return 404*/
          case _ => {
            log.info("returning 404, cached miss for %s", contentUri)
            Future.value(notFound)
          }
        }
      }
    }
  }

  /*do a parallel request to the group proxies, and return the first acceptale request */
  @Trace
  def groupParallelRequest(group: RepositoryGroup, contentUri: String, request: HttpRequest): Future[HttpResponse] = {
    val trackers = group.repos.map {
      repo => {
        val client = clients.get(repo.prefix).get
        log.info("parallel request for %s to %s", contentUri, repo.host)
        /*clone the request and send to the proxied repo that will timeout and return a 504 after 10 seconds*/
        val future = singleRepoRequest(client, contentUri, cloneRequest(request)).within(timer, 10.seconds) handle {
          case _: TimeoutException => timeout
        }
        HitTracker(client, future)
      }
    }

    Future.value(firstAcceptableResponse(trackers)(group, contentUri))

  }

  /*
  return the fisrt acceptable response (200) from the list of requests.
  The list is ordered by the repositories precedence, so we block for the response on the head of the list
  */
  def firstAcceptableResponse(trackers: List[HitTracker])(implicit group: RepositoryGroup, contentUri: String): HttpResponse = {
    trackers.headOption match {
      case Some(tracker) => {
        val response = {
          try {
            /*block for response*/
            tracker.future.get()
          } catch {
            case _ => notFound
          }
        }
        if (response.getStatus.getCode == 200) {
          /*got a good response, cache the repo that gave us this hit, cancel the rest of the requests, and return the response*/
          group.hits += (contentUri -> tracker.client.repo)
          trackers.tail.foreach(_.future.cancel())
          response
        } else {
          firstAcceptableResponse(trackers.tail)
        }
      }
      /*List is empty, we have processed all responses and didnt get any good ones*/
      case None => notFound
    }
  }

  /*
  Make a request to a single proxied repository
  This is called directly from requests to a prefix mapped to a ProxiedRepository, and also to get the response to
  service the request to a prefix mapped to a RepositoryGroup
  Note: request will be mutated to preserve the headers and change the URI.
  */
  @Trace
  def singleRepoRequest(client: Client, contentUri: String, request: HttpRequest): Future[HttpResponse] = {
    val s3request: DefaultHttpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, contentUri)
    s3request.setHeader(HOST, client.repo.bucket + ".s3.amazonaws.com")
    s3request.setHeader(DATE, date)
    s3request.setHeader(AUTHORIZATION, authorization(s3key, s3Secret, s3request, client.repo.bucket))
    /*Check S3 cache first*/
    client.s3Service.service(s3request).flatMap {
      s3response => {
        s3response.getStatus.getCode match {
          /*S3 has the content, return it*/
          case code if (code == 200) => {
            log.info("Serving from S3 bucket %s: %s", client.repo.bucket, contentUri)
            Future.value(s3response)
          }
          /*content not in S3, try to get it from the source repo*/
          case code if (code == 404) => {
            val uri = client.repo.hostPath + contentUri
            request.setUri(uri)
            request.setHeader("Host", client.repo.host)
            client.repoService.service(request).flatMap {
              response => {
                if (response.getStatus == HttpResponseStatus.OK) {
                  /*found the content in the source repo, do an async put of the content to S3*/
                  log.info("Serving from Source %s: %s", client.repo.host, contentUri)
                  val s3buffer = response.getContent.duplicate()
                  putS3(client, request, contentUri, response.getHeader("Content-Type"), s3buffer)
                } else {
                  log.info("Request to Source repo %s: path: %s Status Code: %s", client.repo.host, request.getUri, response.getStatus.getCode)
                }
                Future.value(response)
              }
            }
          }
          case code@_ => {
            log.error("Recieved code: %s", code)
            log.error(s3response.getContent.toString("UTF-8"))
            Future.value(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR))
          }
        }
      }
    }
  }

  /*Asynchronously put content to S3*/
  def putS3(client: Client, request: HttpRequest, contentUri: String, contentType: String, content: ChannelBuffer) {
    val s3Put = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, contentUri)
    s3Put.setContent(content)
    //seems slow and barfs in the log but eventually succeeds. Revisit.
    //s3Put.setHeader("Expect","100-continue")
    s3Put.setHeader(CONTENT_LENGTH, content.readableBytes)
    s3Put.setHeader(CONTENT_TYPE, contentType)
    s3Put.setHeader(HOST, client.repo.bucket + ".s3.amazonaws.com")
    s3Put.setHeader(DATE, date)
    s3Put.setHeader(AUTHORIZATION, authorization(s3key, s3Secret, s3Put, client.repo.bucket))
    client.s3Service.service {
      s3Put
    } onSuccess {
      resp => log.info("S3Put Success: Code %s, Content %s ", resp.getStatus.getReasonPhrase, resp.getContent.toString("UTF-8"))
    } onFailure {
      ex => log.error(ex, "Exception in S3 Put: ")
    } onCancellation {
      log.error("S3Put cancelled %s", s3Put.toString)
    }
  }


  /*get the prefix from the request URI. e.g. /someprefix/some/other/path returns /someprefix */
  def getPrefix(request: HttpRequest): String = {
    val uri = request.getUri.substring(1)
    val index = uri.indexOf("/")
    if (index != -1) {
      "/" + uri.substring(0, index)
    } else {
      "unknown prefix"
    }
  }

  /*get the contentUri from the request URI. e.g. /someprefix/some/path/to/artifact returns /some/path/to/artifact */
  def getContentUri(prefix: String, source: String): String = {
    if (source.contains(prefix)) {
      source.substring(source.indexOf(prefix) + prefix.length())
    } else {
      source
    }
  }
}

object ProxyService {
  /*DateTime format required by AWS*/
  lazy val format = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z").withLocale(java.util.Locale.US).withZone(DateTimeZone.forOffsetHours(0))

  def date: String = format.print(new DateTime)

  val ALGORITHM = "HmacSHA1"
  /*Create the Authorization payload and sign it with the AWS secret*/
  def sign(secret: String, request: HttpRequest, bucket: String): String = {
    val data = List(
      request.getMethod.getName,
      Option(request.getHeader(CONTENT_MD5)).getOrElse(""),
      Option(request.getHeader(CONTENT_TYPE)).getOrElse(""),
      request.getHeader(DATE)
    ).foldLeft("")(_ + _ + "\n") + "/" + bucket + request.getUri
    calculateHMAC(secret, data)
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


case class ProxiedRepository(prefix: String, host: String, hostPath: String, bucket: String, port: Int = 80, ssl: Boolean = false) {
  if (prefix.substring(1).contains("/")) throw new IllegalArgumentException("Prefix %s for Host %s Should not contain the / character, except as its first character".format(prefix, host))
}

case class HitTracker(client: Client, future: Future[HttpResponse])

case class RepositoryGroup(prefix: String, repos: List[ProxiedRepository]) {
  if (prefix.substring(1).contains("/")) throw new IllegalArgumentException("Prefix %s for Group Should not contain the / character, except as its first character".format(prefix))
  val hits = new MMap[String, ProxiedRepository]
  val misses = new MMap[String, DateTime]
}

/*Holds a ProxiedRepository and the associated source and s3 client ServiceFactories*/
case class Client(repoService: ServiceFactory[HttpRequest, HttpResponse], s3Service: ServiceFactory[HttpRequest, HttpResponse], repo: ProxiedRepository)




