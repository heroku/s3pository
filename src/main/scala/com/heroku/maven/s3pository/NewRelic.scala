package com.heroku.maven.s3pository

import java.util.{logging => javalog}
import collection.JavaConversions._
import com.twitter.logging.{Level, Formatter, Handler}
import com.newrelic.api.agent.NewRelic
import com.twitter.logging.config.HandlerConfig
import com.twitter.finagle.stats.{Counter, Stat, StatsReceiverWithCumulativeGauges}

class NewRelicLogHandler(formatter: Formatter, level: Option[Level]) extends Handler(formatter, level) {
  def this() = this(new Formatter(), None)

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

object NewRelicStatsReceiver extends StatsReceiverWithCumulativeGauges {

  val repr: AnyRef = NewRelicStatsReceiver

  protected[this] def deregisterGauge(name: Seq[String]) {}

  protected[this] def registerGauge(name: Seq[String], f: => Float) {}

  def stat(name: String*) = new Stat {
    def add(value: Float) {
      NewRelic.recordMetric(name mkString "/", value)
    }
  }

  def counter(name: String*) = new Counter {
    def incr(delta: Int) {
      NewRelic.incrementCounter(name mkString "/")
    }
  }
}