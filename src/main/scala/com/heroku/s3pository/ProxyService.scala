package com.heroku.s3pository

import com.newrelic.api.agent.Trace

import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.util._
import com.twitter.finagle.http.Http
import com.twitter.finagle.builder.ClientBuilder

import collection.immutable.HashMap
import collection.mutable.{HashMap => MMap}
import collection.JavaConversions._

import java.net.InetSocketAddress

import org.joda.time.DateTime
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._

import com.twitter.finagle.Service
import com.twitter.finagle.stats.StatsReceiver
import java.util.concurrent.atomic.AtomicReference
import annotation.tailrec
import java.lang.IllegalArgumentException
import com.heroku.finagle.aws._
import com.heroku.finagle.aws.S3._

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
class ProxyService(repositories: List[ProxiedRepository], groups: List[RepositoryGroup], doCachePrime: Boolean, s3key: S3Key, s3secret: S3Secret, stats: StatsReceiver) extends Service[HttpRequest, HttpResponse] {

  import ProxyService._

  log.info("creating ProxyService")
  /*Timer used to time box parallel request processing*/
  val timer = new JavaTimer(true)

  val clients: HashMap[String, Client] = {
    repositories.foldLeft(new HashMap[String, Client]) {
      (m, repo) => {
        m + (repo.prefix -> new Client(clientService(repo.host, repo.port, repo.ssl, "source of:" + repo.bucket, stats, 4.seconds, 16.seconds), s3Service(s3key, s3secret, "s3 client for:" + repo.bucket, stats), repo))
      }
    }
  }
  /*create/verify all S3 buckets at creation time*/

  clients.values.foreach(createBucket(_))
  log.warning("S3 Buckets verified")


  val repositoryGroups: HashMap[String, RepositoryGroup] = {
    groups.foldLeft(new HashMap[String, RepositoryGroup]) {
      (m, g) => m + (g.prefix -> g)
    }
  }

  if (doCachePrime) {
    repositoryGroups.values.foreach(primeHitCaches(_))
    log.warning("Hit Cache populated")
  }

  def primeHitCaches(group: RepositoryGroup) {
    group.repos.reverse.foreach {
      repo =>
        log.debug("priming hit cache from %s", repo.bucket)
        ListBucket.getKeys(clients.get(repo.prefix).get.s3Service.service, repo.bucket).foreach(key => group.hits += (("/" + key) -> repo))
    }
  }


  /*Create any missing S3 buckets. Create bucket is idempotent, and returns a 200 if the bucket exists or is created*/
  def createBucket(client: Client) {
    log.debug("creating bucket: %s".format(client.repo.bucket))
    client.s3Service(CreateBucket(client.repo.bucket)) onSuccess {
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
    }
  }

  /*main service function for ProxyService, this handles all incoming requests*/
  @Trace(dispatcher = true)
  def apply(request: HttpRequest) = {
    log.info("Request for: %s", request.getUri)
    val prefix = getPrefix(request)
    val contentUri = getContentUri(prefix, request.getUri)
    repositoryGroups.get(prefix) match {
      /*request matches a group*/
      case Some(group) => {
        log.info("Group request: %s", group.prefix)
        groupRepoRequest(group, contentUri, request).onSuccess(inspectFinalResponse(request, _)).onFailure(inspectFinalError(request, _))
      }
      case None => {
        clients.get(prefix) match {
          /*request matches a single proxied repo*/
          case Some(client) => {
            log.info("Single repo request: %s", prefix)
            singleRepoRequest(client, contentUri, request).onSuccess(inspectFinalResponse(request, _)).onFailure(inspectFinalError(request, _))
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
        log.info("Cache hit %s on %s", contentUri, proxiedRepo.host)
        singleRepoRequest(clients.get(proxiedRepo.prefix).get, contentUri, request)
      }
      /*group dosent have a hit, try and find the contentUri in one of the groups proxies*/
      case None => {
        group.misses.get(contentUri) match {
          /*no cached misses, do a parallel request to the group proxies*/
          case None => groupParallelRequest(group, contentUri, request)
          /*cached missed is timed out, remove the cache entry and do a parallel request to the group proxies*/
          case Some(time) if (time.plusMinutes(group.missTimeout).isBeforeNow) => {
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
    val requests: List[Future[(HttpResponse, Client)]] = group.repos.map {
      repo => {
        val client = clients.get(repo.prefix).get
        log.debug("parallel request for %s to %s", contentUri, repo.host)
        /*clone the request and send to the proxied repo that will timeout and return a 504 after 30 seconds*/
        singleRepoRequest(client, contentUri, cloneRequest(request)).within(timer, 30.seconds).handle {
          case t: TimeoutException => {
            log.warning(t, "timeout in parallel req to %s for %s", client.repo.host, contentUri)
            timeout
          }
          case ex: Exception => {
            log.error(ex, "error in parallel req to %s for %s", client.repo.host, contentUri)
            timeout
          }
        }.map(resp => (resp, client))
      }
    }

    Future.value(firstAcceptableResponse(requests)(group, contentUri, notFound))

  }

  /*
  return the fisrt acceptable response (200) from the list of requests.
  */
  def firstAcceptableResponse(requests: Seq[Future[(HttpResponse, Client)]])(implicit group: RepositoryGroup, contentUri: String, fallbackResponse: HttpResponse): HttpResponse = {
    requests.headOption match {
      case Some(_) => {
        val (first, rest) = Future.select(requests).get()
        if (first.isReturn) {
          val (response, client) = first.get()
          if (response.getStatus.getCode == 200) {
            /*got a good response, cache the repo that gave us this hit, cancel the rest of the requests, and return the response*/
            log.debug("Parallel winner: %s for %s", client.repo.host, contentUri)
            group.hits += (contentUri -> client.repo)
            rest.foreach(_.cancel())
            response
          } else if (response.getStatus.getCode == 504) {
            firstAcceptableResponse(rest)(group, contentUri, timeout)
          } else {
            firstAcceptableResponse(rest)
          }
        } else {
          log.warning("Exception in parallel retrieve, skipping")
          firstAcceptableResponse(rest)
        }
      }
      case None => {
    	  if(fallbackResponse.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {    	    
    		  group.misses.put(contentUri, new DateTime())
    	  }
    	  fallbackResponse
      }
        
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
    if (client.repo.canContain(contentUri)) {
      /*Check S3 cache first*/
      client.s3Service.tryService(Get(client.repo.bucket, contentUri), timeout, client.repo.bucket)("error checking s3 bucket %s for %s ", client.repo.bucket, contentUri).flatMap {
        s3response => {
          s3response.getStatus.getCode match {
            /*S3 has the content, return it */
            case code if (code == 200 && s3response.hasContent) => {
              log.info("Serving from S3 bucket %s: %s", client.repo.bucket, contentUri)
              Future.value(s3response)
            }
            /*content not in S3 or s3 not responding in time, try to get it from the source repo*/
            /*
            200s with no content can happen in strange cases like an object whose key starts
            with 'soap/' will return a 200 with no content even though there is no object there
            */
            case code if (code == 404 || code == 504 || code == 500 || (code == 200 && s3response.hasNoContent)) => {
              val uri = client.repo.hostPath + contentUri
              request.setUri(uri)
              request.setHeader(HOST, client.repo.host)
              client.repoService.tryService(request, timeout, client.repo.host)("error checking source repo %s for %s ", client.repo.host, contentUri).flatMap {
                response => {
                  if (response.getStatus == HttpResponseStatus.OK && (request.getMethod equals HttpMethod.GET) && (code == 404 || code == 200)) {
                    /*found the content in the source repo, do an async put of the content to S3*/
                    log.info("Serving from Source %s: %s", client.repo.host, contentUri)
                    val s3buffer = response.getContent.duplicate()
                    putS3(client, contentUri, response, s3buffer)
                  } else {
                    /*504s happen frequently when we time out the singleRepoRequests in a group request*/
                    if (response.getStatus.getCode == 200 || response.getStatus.getCode == 404 || response.getStatus.getCode == 504) {
                      log.info("Request to Source repo %s: path: %s Status Code: %s", client.repo.host, request.getUri, response.getStatus.getCode)
                    } else {
                      log.warning(new RuntimeException("Unexpected upstream response"), "Request to Source repo %s: path: %s Status Code: %s", client.repo.host, request.getUri, response.getStatus.getCode)
                    }
                  }
                  Future.value(response)
                }
              }
            }
            case code@_ => {
              log.error(new RuntimeException("error code recieved from upstream" + client.repo.host), "Recieved code: %s", code)
              log.error(s3response.getContent.toString("UTF-8"))
              Future.value(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR))
            }
          }
        }
      }
    } else {
      log.info("Skip looking for %s in %s / %s", contentUri, client.repo.bucket, client.repo.host)
      Future.value(notFound)
    }
  }


  /*Asynchronously put content to S3*/
  def putS3(client: Client, contentUri: String, response: HttpResponse, content: ChannelBuffer) {
    val s3Put = Put(client.repo.bucket, contentUri, content, STORAGE_CLASS -> RRS)
    response.ifHeader(ETAG)(s3Put.setHeader(SOURCE_ETAG, _))
    response.ifHeader(LAST_MODIFIED)(s3Put.setHeader(SOURCE_MOD, _))
    client.s3Service {
      s3Put
    } onSuccess {
      resp => {
        if (resp.getStatus.getCode == 200) {
          log.info("S3Put Success: Code %s, Content %s ", resp.getStatus.getReasonPhrase, resp.getContent.toString("UTF-8"))
        } else {
          log.error(new RuntimeException(resp.getContent.toString("UTF-8")), "S3Put did not return a 200")
        }
      }
    } onFailure {
      ex => log.error(ex, "Exception in S3 Put: ")
    }
  }

  /*get the prefix from the request URI. e.g. /someprefix/some/other/path returns /someprefix */
  def getPrefix(request: HttpRequest): String = {
    val uri = request.getUri.substring(1)
    getPrefix(uri)
  }

  def getPrefix(uri: String): String = {
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

  def inspectFinalResponse(req: HttpRequest, resp: HttpResponse) {
    if (resp.getStatus.getCode != 200 && resp.getStatus.getCode != 404) {
      log.warning(new RuntimeException("Unexpected Final Response Code"), "Request for %s: Status Code: %s", req.getUri, resp.getStatus.getCode)
    }
  }

  def inspectFinalError(req: HttpRequest, t: Throwable) {
    log.error(t, "Request for %s: Threw: %s", req.getUri, t.getClass.getSimpleName)
  }
}

object ProxyService {
  val log = Logger.get(classOf[ProxyService])

  val STORAGE_CLASS = "x-amz-storage-class"
  val RRS = "REDUCED_REDUNDANCY"
  val SOURCE_ETAG = "x-amz-meta-source-etag"
  val SOURCE_MOD = "x-amz-meta-source-mod"

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

  /*Build a Client ServiceFactory for the given endpoint*/
  def clientService(host: String, port: Int, ssl: Boolean, name: String, stats: StatsReceiver, connectTimeout: Duration = 2.second, requestTimeout: Duration = 8.seconds): Service[HttpRequest, HttpResponse] = {
    import com.twitter.conversions.storage._
    var builder = ClientBuilder()
      .codec(Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabyte))
      .sendBufferSize(262144)
      .recvBufferSize(262144)
      .hosts(new InetSocketAddress(host, port))
      .hostConnectionLimit(Integer.MAX_VALUE)
      .retries(1)
      .requestTimeout(requestTimeout)
      .tcpConnectTimeout(connectTimeout)
      .reportTo(stats)
      .name(name)
    if (ssl) (builder = builder.tlsWithoutValidation())
    builder.build()
  }

  def s3Service(s3key: S3Key, s3Secret: S3Secret, name: String, stats: StatsReceiver, connectTimeout: Duration = 2.second, requestTimeout: Duration = 8.seconds): Service[S3Request, HttpResponse] = {
    import com.twitter.conversions.storage._
    ClientBuilder().codec(S3(s3key.key, s3Secret.secret, Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabytes)))
      .sendBufferSize(262144)
      .recvBufferSize(262144)
      .hosts("s3.amazonaws.com:80")
      .hostConnectionLimit(Integer.MAX_VALUE)
      .requestTimeout(requestTimeout)
      .tcpConnectTimeout(connectTimeout)
      .name(name)
      .retries(1)
      .reportTo(stats)
      .build()
  }
}

case class ProxiedRepository(prefix: String, host: String, hostPath: String, bucket: String, port: Int = 80, ssl: Boolean = false, _includes: List[String] = List.empty[String]) {
  if (prefix.substring(1).contains("/")) throw new IllegalArgumentException("Prefix %s for Host %s Should not contain the / character, except as its first character".format(prefix, host))

  def include(prefix: String) = this.copy(_includes = (prefix :: this._includes))

  def canContain(contentUri: String): Boolean = {
    if (contentUri == "/") false
    else if (_includes.size == 0) true
    else !skip(_includes, contentUri)
  }

  @tailrec
  private def skip(paths: List[String], contentUri: String): Boolean = {
    paths.headOption match {
      case Some(include) if (contentUri.startsWith(include)) => false
      case None => true
      case _ => skip(paths.tail, contentUri)
    }
  }
}

case class RepositoryGroup(prefix: String, repos: List[ProxiedRepository], missTimeout: Int=30) {
  if (prefix.substring(1).contains("/")) throw new IllegalArgumentException("Prefix %s for Group Should not contain the / character, except as its first character".format(prefix))
  val hits = new MMap[String, ProxiedRepository]
  val misses = new MMap[String, DateTime]
}

/*Holds a ProxiedRepository and the associated source and s3 client ServiceFactories*/
class Client(repoServiceFactory: => Service[HttpRequest, HttpResponse], s3ServiceFactory: => Service[S3Request, HttpResponse], val repo: ProxiedRepository) {
  val repoRef: AtomicReference[Service[HttpRequest, HttpResponse]] = new AtomicReference[Service[HttpRequest, HttpResponse]](repoServiceFactory)
  val s3Ref: AtomicReference[Service[S3Request, HttpResponse]] = new AtomicReference[Service[S3Request, HttpResponse]](s3ServiceFactory)

  def repoService: Service[HttpRequest, HttpResponse] = {
    get(repoRef, repoServiceFactory, repo.host)
  }

  def s3Service: Service[S3Request, HttpResponse] = {
    get(s3Ref, s3ServiceFactory, repo.bucket)
  }

  private def get[T <: Service[_, _]](ref: AtomicReference[T], fact: => T, msg: String): T = {
    val svc = ref.get()
    if (svc.isAvailable) svc
    else {
      log.warning("%s: service was unavailable", msg)
      val newSvc = fact
      if (ref.compareAndSet(svc, newSvc)) {
        log.warning("%s: cas-ed new service, releasing old one", msg)
        svc.release()
        newSvc
      } else {
        log.warning("%s: cas of new service failed, releasing new service and calling get again", msg)
        newSvc.release()
        get(ref, fact, msg)
      }
    }
  }


}





