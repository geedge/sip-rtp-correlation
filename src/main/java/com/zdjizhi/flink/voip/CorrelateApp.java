package com.zdjizhi.flink.voip;

import com.zdjizhi.flink.voip.conf.FusionConfiguration;
import com.zdjizhi.flink.voip.error.ErrorHandler;
import com.zdjizhi.flink.voip.formats.JsonNodeSerializationSchema;
import com.zdjizhi.flink.voip.functions.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;

import static com.zdjizhi.flink.voip.conf.FusionConfigs.*;

/**
 * The main class for running the SIP RTP Correlation application.
 *
 * @author chaoc
 * @since 1.0
 */
public class CorrelateApp {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // param check
        if (args.length < 1) {
            throw new IllegalArgumentException("Error: Not found properties path. " +
                    "\nUsage: flink -c xxx xxx.jar app.properties.");
        }

        final ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);

        final Configuration config = tool.getConfiguration();
        env.getConfig().setGlobalJobParameters(config);

        final FusionConfiguration fusionConfiguration = new FusionConfiguration(config);

        final FlinkKafkaConsumer<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer<>(
                config.get(SOURCE_KAFKA_TOPIC),
                new JsonNodeDeserializationSchema(),
                fusionConfiguration
                        .getProperties(SOURCE_KAFKA_PROPERTIES_PREFIX));

        final DataStream<ObjectNode> sourceStream = env.addSource(kafkaConsumer)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ObjectNode>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((SerializableTimestampAssigner<ObjectNode>)
                                        (element, recordTimestamp) ->
                                                element.get("common_start_timestamp_ms").asLong()));

        final ErrorHandler errorHandler = new ErrorHandler(config);

        // Process the data using the TypeSplitFunction and split it into separate DataStreams for SIP and RTP data
        final SingleOutputStreamOperator<ObjectNode> splitsRecordsOperator =
                errorHandler.filterError(sourceStream)
                        .process(new TypeSplitFunction())
                        .name("SplitsRecordsBasedSchemaType")
                        .uid("splits-records-based-schema-type");

        // Get the DataStreams for SIP and RTP data from the side outputs.
        final DataStream<ObjectNode> sipDataStream = splitsRecordsOperator
                .getSideOutput(TypeSplitFunction.SIP_OUTPUT_TAG);

        final DataStream<ObjectNode> rtpDataStream = splitsRecordsOperator
                .getSideOutput(TypeSplitFunction.RTP_OUTPUT_TAG);

        // Process SIP data to create a double directional stream using SIPPairingFunction.
        final SingleOutputStreamOperator<ObjectNode> sipDoubleDirOperator = sipDataStream
                .keyBy(new SIPKeySelector())
                .process(new SIPPairingFunction())
                .name("PairingOneWayToDoubleStream")
                .uid("pairing-one-way-to-double");

        final KeySelector<ObjectNode, Tuple2<Integer, Address>> vSysSelector = new VSysIDKeySelector();

        // Fusion SIP data and RTP data to VoIP data.
        final SingleOutputStreamOperator<ObjectNode> voIpOperator = rtpDataStream
                .keyBy(vSysSelector)
                .connect(sipDoubleDirOperator.keyBy(vSysSelector))
                .process(new VoIPFusionFunction())
                .name("VoIPFusion")
                .uid("voip-fusion");

        final FlinkKafkaProducer<ObjectNode> producer = new FlinkKafkaProducer<>(
                config.get(SINK_KAFKA_TOPIC),
                new JsonNodeSerializationSchema(),
                fusionConfiguration.getProperties(SINK_KAFKA_PROPERTIES_PREFIX));

        voIpOperator.union(sipDoubleDirOperator).addSink(producer);

        env.execute("SIP-RTP-CORRELATION");
    }
}
