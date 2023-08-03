package com.zdjizhi.flink.voip.error;

import com.zdjizhi.flink.voip.conf.FusionConfigs;
import com.zdjizhi.flink.voip.conf.FusionConfiguration;
import com.zdjizhi.flink.voip.formats.JsonNodeSerializationSchema;
import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SIPRecord;
import com.zdjizhi.flink.voip.records.SchemaType;
import com.zdjizhi.flink.voip.records.StreamDir;
import com.zdjizhi.utils.IPUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ErrorHandler class is responsible for handling and filtering error records from the input data stream.
 * It checks for records that have invalid or meaningless addresses and ports, and outputs them to a separate stream if enabled.
 *
 * @author chaoc
 * @since 1.0
 */
public class ErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorHandler.class);
    /**
     * The OutputTag for invalid records.
     */
    public static final OutputTag<ObjectNode> INVALID_OUTPUT_TAG =
            new OutputTag<>("invalid-records");

    private final Configuration config;

    /**
     * Creates a new ErrorHandler instance with the given configuration.
     *
     * @param config The configuration containing settings for error handling.
     */
    public ErrorHandler(final Configuration config) {
        this.config = config;
    }

    /**
     * Filters out error records from the input data stream and outputs them to a separate stream if enabled.
     *
     * @param dataStream The input data stream of ObjectNode records.
     * @return A new DataStream containing valid records without errors.
     */
    public DataStream<ObjectNode> filterError(final DataStream<ObjectNode> dataStream) {
        // Process the data stream to identify meaningless addresses and ports
        final SingleOutputStreamOperator<ObjectNode> operator = dataStream
                .process(new MeaninglessAddressProcessFunction())
                .name("MeaninglessRecords")
                .uid("meaningless-records");

        // If enabled, output the error records to the specified Kafka topic
        if (config.get(FusionConfigs.ERROR_RECORDS_OUTPUT_ENABLE)) {

            final String topic = config.get(FusionConfigs.ERROR_SINK_KAFKA_TOPIC);

            LOG.info("Meaningless data output is enabled, data will be sent to: Topic [{}]", topic);

            final DataStream<ObjectNode> errorStream = operator
                    .getSideOutput(INVALID_OUTPUT_TAG);

            final FlinkKafkaProducer<ObjectNode> producer = new FlinkKafkaProducer<>(
                    topic,
                    new JsonNodeSerializationSchema(),
                    new FusionConfiguration(config)
                            .getProperties(FusionConfigs.ERROR_SINK_KAFKA_PROPERTIES_PREFIX)
            );

            errorStream.addSink(producer);
        }
        return operator;
    }
}

/**
 * The MeaninglessAddressProcessFunction is a custom ProcessFunction used to check for records
 * with invalid or meaningless addresses and ports. It separates them into the invalid output tag if necessary.
 */
class MeaninglessAddressProcessFunction extends ProcessFunction<ObjectNode, ObjectNode> {

    private static final Logger LOG = LoggerFactory.getLogger(MeaninglessAddressProcessFunction.class);

    @Override
    public void processElement(ObjectNode obj,
                               ProcessFunction<ObjectNode, ObjectNode>.Context ctx,
                               Collector<ObjectNode> out) throws Exception {
        final Record record = new Record(obj);
        // Check for invalid or meaningless addresses and ports
        boolean cond1 = StringUtils.isNotBlank(record.getClientIp()) &&
                StringUtils.isNotBlank(record.getServerIp()) &&
                record.getClientPort() >= 0 &&
                record.getServerPort() >= 0;

        final SIPRecord sipRecord = new SIPRecord(obj);
        boolean cond2 = !IPUtil.isIPAddress(sipRecord.getOriginatorSdpConnectIp())
                || IPUtil.internalIp(sipRecord.getOriginatorSdpConnectIp());
        boolean cond3 = !IPUtil.isIPAddress(sipRecord.getResponderSdpConnectIp())
                || IPUtil.internalIp(sipRecord.getResponderSdpConnectIp());
        boolean cond4 = IPUtil.isIPAddress(sipRecord.getOriginatorSdpConnectIp())
                || IPUtil.isIPAddress(sipRecord.getResponderSdpConnectIp());
        boolean cond5 = SchemaType.SIP.getValue().equalsIgnoreCase(sipRecord.getSchemaType());
        boolean cond6 = StreamDir.DOUBLE.getValue() == sipRecord.getStreamDir() &&
                IPUtil.internalIp(sipRecord.getResponderSdpConnectIp()) &&
                IPUtil.internalIp(sipRecord.getOriginatorSdpConnectIp());

        // Both client and server addresses in the data are valid.
        if (cond1 && (
                // The address in the SIP one-way stream is valid and not an internal network address.
                cond2 && cond3 && cond4 && cond5
                        // The coordinating addresses in the SIP double directional stream are valid
                        // and not internal network addresses.
                        || cond5 && cond6
                        || !cond5)) {
            out.collect(obj);
        } else {
            // Output invalid records to the invalid output tag
            ctx.output(ErrorHandler.INVALID_OUTPUT_TAG, obj);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Meaningless record: {}", obj);
            }
        }
    }
}