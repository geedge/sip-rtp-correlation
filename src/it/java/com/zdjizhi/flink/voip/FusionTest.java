package com.zdjizhi.flink.voip;

import com.zdjizhi.flink.voip.functions.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

// Integration test main class
public class FusionTest {

    public static void main(String[] args) throws Exception {
        final Configuration envConfig = new Configuration();
        envConfig.setInteger("rest.port", 18081);
        envConfig.setBoolean("rest.flamegraph.enabled", true);
        envConfig.setString("metrics.reporter.influxdb.factory.class", "org.apache.flink.metrics.influxdb.InfluxdbReporterFactory");
        envConfig.setString("metrics.reporter.influxdb.scheme", "http");
        envConfig.setString("metrics.reporter.influxdb.host", "127.0.0.1");
        envConfig.setInteger("metrics.reporter.influxdb.port", 8086);
        envConfig.setString("metrics.reporter.influxdb.db", "flinks");
        envConfig.setString("metrics.reporter.influxdb.interval", "5 SECONDS");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(envConfig);

        final ParameterTool tool = ParameterTool.fromPropertiesFile(
                args.length > 0 ? args[0] :
                        Objects.requireNonNull(
                                FusionTest.class.getResource("/application-test.properties")).getPath()
        );

        final CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        env.enableCheckpointing(Time.minutes(1)
                .toMilliseconds(), CheckpointingMode.AT_LEAST_ONCE);
        checkpointConfig.setCheckpointTimeout(Time.minutes(2).toMilliseconds());
        checkpointConfig.setCheckpointStorage("file://" + System.getProperty("java.io.tmpdir"));
       /* checkpointConfig.setCheckpointStorage(
                Objects.requireNonNull(
                        FusionTest.class.getResource("/")).toString());*/

        final Configuration config = tool.getConfiguration();
        env.getConfig().setGlobalJobParameters(config);
        env.setParallelism(8);

        final SingleOutputStreamOperator<ObjectNode> sourceStream = env.addSource(new DataGenSource())
                .name("DataGenSource")
                .setParallelism(1);

        // Process the data using the TypeSplitFunction and split it into separate DataStreams for SIP and RTP data
        final SingleOutputStreamOperator<ObjectNode> splitsRecordsOperator =
                sourceStream
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

        voIpOperator.addSink(new DoNothingSink())
                .name("DoNothingSink")
                .setParallelism(1);

        env.execute("VoIP Fusion Job");
    }
}
