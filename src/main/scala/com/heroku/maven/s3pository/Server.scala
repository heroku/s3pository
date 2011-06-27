package com.heroku.maven.s3pository

import com.twitter.finagle.http.Http
import com.twitter.finagle.Service
import java.net.InetSocketAddress
import com.twitter.finagle.builder.{ServerBuilder, ClientBuilder}
import com.twitter.conversions.storage._
import util.Properties
import collection.immutable.HashMap
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.{Return, Promise, Future}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import javax.crypto.spec.SecretKeySpec
import javax.crypto.Mac
import org.jboss.netty.handler.codec.http._
import java.util.logging.{LogManager, Logger}

object S3Server {
  def main(args: Array[String]) {
    LogManager.getLogManager.readConfiguration(getClass.getClassLoader.getResourceAsStream("logging.properties"))
    val log = Logger.getLogger("S3Server-Main")
    val central = new ProxiedRepository("/central", "repo1.maven.org", "/maven2", "sclasen-proxy-central")
    val akka = new ProxiedRepository("/akka", "akka.io", "/repository", "sclasen-proxy-akka")
    val address = new InetSocketAddress(Properties.envOrElse("PORT", "8080").toInt)
    val s3key: String = Properties.envOrNone("S3_KEY").getOrElse {
      log.severe("S3_KEY env var not defined, exiting")
      System.exit(666)
      "noKey"
    }
    val s3secret: String = Properties.envOrNone("S3_SECRET").getOrElse {
      log.severe("S3_SECRET env var not defined, exiting")
      System.exit(666)
      "noSecret"
    }
    val service = new ProxyService(List(central, akka), s3key, s3secret)
    val server = ServerBuilder()
      .codec(Http(_maxRequestSize = 100.megabytes, _maxResponseSize = 100.megabyte))
      .bindTo(address)
      .sendBufferSize(1048576)
      .recvBufferSize(1048576)
      .name("s3pository")
      .logger(Logger.getLogger("S3Server"))
      .build(service)
  }
}

case class ProxiedRepository(prefix: String, host: String, hostPath: String, bucket: String)

case class Client(repoService: Service[HttpRequest, HttpResponse], s3Service: Service[HttpRequest, HttpResponse], repo: ProxiedRepository)

class ProxyService(repositories: List[ProxiedRepository], s3key: String, s3Secret: String) extends Service[HttpRequest, HttpResponse] {

  import ProxyService._

  val log = Logger.getLogger(getClass.getName)

  val clients: HashMap[String, Client] = {
    var map = new HashMap[String, Client]
    repositories.foldLeft(map) {
      (m, repo) => {
        m + (repo.prefix -> Client(service(repo.host), service(repo.bucket + ".s3.amazonaws.com"), repo))
      }
    }
  }

  def service(host: String): Service[HttpRequest, HttpResponse] = {
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
    val clientOpt = getClient(request)
    clientOpt match {
      case None => Future.value(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND))
      case Some(client) => {
        val content = getContentUri(client, request)
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
    }
  }

  def putS3(client: Client, request: HttpRequest, contentUri: String, contentType: String, content: ChannelBuffer) {
    val s3Put = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, contentUri)
    s3Put.setContent(content)
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

  def getClient(reqest: HttpRequest): Option[Client] = {
    clients.get {
      val uri = reqest.getUri.substring(1)
      "/" + uri.substring(0, uri.indexOf("/"))
    }
  }

  def getContentUri(client: Client, request: HttpRequest): String = {
    val source = request.getUri
    val prefix = client.repo.prefix
    source.substring(source.indexOf(prefix) + prefix.length())
  }
}

object ProxyService {
  lazy val format = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z").withLocale(java.util.Locale.US).withZone(DateTimeZone.forOffsetHours(0))

  def date: String = format.print(new DateTime)

  val ALGORITHM = "HmacSHA1"
}






