package com.heroku.maven.s3pository

import com.newrelic.api.agent.NewRelic
import com.twitter.finagle.stats._
import com.twitter.finagle.tracing.{Span, TraceReceiver}

/**
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 7/6/11
 * Time: 12:07 PM
 * To change this template use File | Settings | File Templates.
 */

object NewRelicStatsReceiver extends StatsReceiverWithCumulativeGauges {

  protected[this] def deregisterGauge(name: Seq[String]) {}

  protected[this] def registerGauge(name: Seq[String], f: => Float) {}

  def stat(name: String*) = new Stat{
    def add(value: Float) {
      NewRelic.recordMetric(name mkString "/",value)
    }
  }

  def counter(name: String*) = new Counter{
    def incr(delta: Int) {
      NewRelic.incrementCounter(name mkString "/")
    }
  }
}
