package com.heroku.maven.s3pository

import java.util.{logging => javalog}
import collection.JavaConversions._
import com.twitter.logging.{BasicFormatter, Level, Formatter, Handler}
import com.newrelic.api.agent.NewRelic
import com.twitter.logging.config.HandlerConfig


class NewRelicLogHandler(formatter: Formatter, level: Option[Level]) extends Handler(formatter, level) {
  def this() = this (BasicFormatter, None)

  def publish(record: javalog.LogRecord) {
    Option(record.getThrown) match {
      case Some(exception) => {
        val map = Map(
          "thread" -> record.getThreadID.toString,
          "logger" -> record.getLoggerName,
          "sourceClass" -> record.getSourceClassName,
          "sourceMethod" -> record.getSourceMethodName,
          "message" -> record.getMessage
        )
        NewRelic.noticeError(exception, asJavaMap(map))
      }
      case None => ()
    }
  }

  def close() = {}

  def flush() = {}
}

class NewRelicLogHandlerConfig extends HandlerConfig {
  def apply() = new NewRelicLogHandler(formatter(), level)
}