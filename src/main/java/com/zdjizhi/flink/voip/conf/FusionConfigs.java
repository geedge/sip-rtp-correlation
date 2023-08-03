package com.zdjizhi.flink.voip.conf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Containing configuration options for the Fusion application.
 *
 * @author chaoc
 * @since 1.0
 */
public class FusionConfigs {

    /**
     * The prefix for Kafka properties used in the source.
     */
    public static final String SOURCE_KAFKA_PROPERTIES_PREFIX = "source.kafka.props.";

    /**
     * The prefix for Kafka properties used in the sink.
     */
    public static final String SINK_KAFKA_PROPERTIES_PREFIX = "sink.kafka.props.";

    /**
     * Configuration option for the Kafka topic used in the source.
     */
    public static final ConfigOption<String> SOURCE_KAFKA_TOPIC =
            ConfigOptions.key("source.kafka.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Kafka topic used in the source.");

    /**
     * Configuration option for the Kafka topic used in the sink.
     */
    public static final ConfigOption<String> SINK_KAFKA_TOPIC =
            ConfigOptions.key("sink.kafka.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Kafka topic used in the sink.");

    /**
     * The configuration option for the interval at which SIP state data
     * should be cleared.
     */
    public static final ConfigOption<Integer> SIP_STATE_CLEAR_INTERVAL =
            ConfigOptions.key("sip.state.clear.interval.minutes")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The interval at which SIP state data should be cleared.");

    /**
     * The configuration option for the interval at which STP state data
     * should be cleared.
     */
    public static final ConfigOption<Integer> RTP_STATE_CLEAR_INTERVAL =
            ConfigOptions.key("rtp.state.clear.interval.minutes")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The interval at which RTP state data should be cleared.");
}