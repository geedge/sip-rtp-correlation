package com.zdjizhi.flink.voip.conf

import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}

import java.lang.{Long => JLong}

/**
 * An object containing configuration options for the Fusion application.
 *
 * @author chaoc
 * @since 1.0
 */
object FusionConfigs {

  val SOURCE_KAFKA_TOPIC: ConfigOption[String] =
    ConfigOptions.key("source.kafka.topic")
      .stringType()
      .noDefaultValue()
      .withDescription("")

  val SINK_KAFKA_TOPIC: ConfigOption[String] =
    ConfigOptions.key("sink.kafka.topic")
      .stringType()
      .noDefaultValue()
      .withDescription("")

  val SOURCE_KAFKA_PROPERTIES_PREFIX: String = "source.kafka.props."

  val SINK_KAFKA_PROPERTIES_PREFIX: String = "sink.kafka.props."

  /**
   * The configuration option for the interval at which SIP (Session Initiation Protocol) state data
   * should be cleared.
   */
  val SIP_STATE_CLEAR_INTERVAL: ConfigOption[JLong] =
    ConfigOptions.key("")
      .longType()
      .defaultValue(Time.minutes(1).toMilliseconds)
      .withDescription("")

  val RTP_STATE_CLEAR_INTERVAL: ConfigOption[JLong] =
    ConfigOptions.key("")
      .longType()
      .defaultValue(Time.minutes(3).toMilliseconds)
      .withDescription("")

}
