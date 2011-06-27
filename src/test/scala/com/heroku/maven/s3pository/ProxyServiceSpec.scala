package com.heroku.maven.s3pository

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.jboss.netty.handler.codec.http.{HttpMethod, HttpVersion, DefaultHttpRequest}

class ProxyServiceSpec extends WordSpec with MustMatchers {

  /*"A ProxySerice" must {
    "translate source to target uris correctly " in  {
      val p = new ProxyService(List(ProxiedRepository("/localprefix", "repo1.maven.org", "/maven2","bucket")),"key","secret")
      p.getContentUri(new DefaultHttpRequest(HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "http://localfoo/localprefix/log4j/log4j/1.2.16/log4j-1.2.16.pom")) must be("/maven2/log4j/log4j/1.2.16/log4j-1.2.16.pom")

    }
  }
*/
}