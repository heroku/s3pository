package com.heroku.maven.s3pository

import com.heroku.maven.s3pository.LangPackSwitch._
import com.heroku.maven.s3pository.LangPackUpdate._
import com.heroku.maven.s3pository.S3rver._

object PlayLangPack {
  val bucket = s3prefix + "-langpack-play"
}

object PlayOn {
  val from = "/ivysettings-proxy.xml"
  val to = "/ivysettings.xml"

  def main(args: Array[String]) {
    LangPackSwitch.doSwitch(PlayLangPack.bucket, from, to)
  }
}

object PlayOff {
  val from = "/ivysettings-noproxy.xml"
  val to = "/ivysettings.xml"

  def main(args: Array[String]) {
    LangPackSwitch.doSwitch(PlayLangPack.bucket, from, to)
  }
}

object PlayUpdate {
  
  def main(args: Array[String]) {
    val settingsProxyBuffer = LangPackUpdate.readFile("language-pack-play/ivysettings-proxy.xml");
    val settingsNoProxyBuffer = LangPackUpdate.readFile("language-pack-play/ivysettings-noproxy.xml")
    LangPackUpdate.putFile(PlayLangPack.bucket, "/ivysettings-proxy.xml", settingsProxyBuffer)
    LangPackUpdate.putFile(PlayLangPack.bucket, "/ivysettings-noproxy.xml", settingsNoProxyBuffer)
  }
}