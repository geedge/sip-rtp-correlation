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
     * Configuration prefix for the properties of the Kafka sink where the error data will be output.
     */
    public static final String ERROR_SINK_KAFKA_PROPERTIES_PREFIX = "error.sink.kafka.props.";

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
     * Configuration option to enable or disable the output of error records.
     * If set to true, the error records will be sent to the specified Kafka topic.
     * Default value is false.
     */
    public static final ConfigOption<Boolean> ERROR_RECORDS_OUTPUT_ENABLE =
            ConfigOptions.key("error.records.output.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable or disable the output of error records. " +
                            "If set to true, the error records will be sent to the specified Kafka topic.");

    /**
     * Configuration option to determine whether to perform data correlate for intranet addresses.
     */
    public static final ConfigOption<Boolean> INCLUDE_INTRANET_IP =
            ConfigOptions.key("include.intranet.ip")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to perform data correlate for intranet addresses");


    /**
     * Configuration option for specifying the Kafka topic name where the error data will be sent.
     * This configuration option is used when the output of error records is enabled.
     */
    public static final ConfigOption<String> ERROR_SINK_KAFKA_TOPIC =
            ConfigOptions.key("error.sink.kafka.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Kafka topic name where the error records will be sent.");

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
                    .defaultValue(6)
                    .withDescription("The interval at which RTP state data should be cleared.");

    /**
     * Configuration option for specifying the name of a job.
     */
    public static final ConfigOption<String> JOB_NAME =
            ConfigOptions.key("job.name")
                    .stringType()
                    .defaultValue("correlation_sip_rtp_session")
                    .withDescription("The name of current job.");
}