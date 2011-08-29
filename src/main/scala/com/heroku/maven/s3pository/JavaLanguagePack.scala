package com.heroku.maven.s3pository

import com.heroku.maven.s3pository.LangPackSwitch._
import com.heroku.maven.s3pository.LangPackUpdate._
import com.heroku.maven.s3pository.S3rver._

object JavaLangPack {
  val bucket = s3prefix + "-langpack-java"
}

object JavaOn {
  val from = "/settings-proxy.xml"
  val to = "/settings.xml"

  def main(args: Array[String]) {
    LangPackSwitch.doSwitch(JavaLangPack.bucket, from, to)
  }
}

object JavaOff {
  val from = "/settings-noproxy.xml"
  val to = "/settings.xml"

  def main(args: Array[String]) {
    LangPackSwitch.doSwitch(JavaLangPack.bucket, from, to)
  }
}

object JavaUpdate {
  
  def main(args: Array[String]) {
    val settingsProxyBuffer = LangPackUpdate.readFile("language-pack-java/settings-proxy.xml");
    val settingsNoProxyBuffer = LangPackUpdate.readFile("language-pack-java/settings-noproxy.xml")
    LangPackUpdate.putFile(JavaLangPack.bucket, "/settings-proxy.xml", settingsProxyBuffer)
    LangPackUpdate.putFile(JavaLangPack.bucket, "/settings-noproxy.xml", settingsNoProxyBuffer)
  }
}