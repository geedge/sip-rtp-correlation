package com.zdjizhi.flink.voip;

import com.zdjizhi.flink.voip.functions.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

// Integration test main class
public class FusionTest {

    public static void main(String[] args) throws Exception {
        final Configuration envConfig = new Configuration();
        envConfig.setInteger("rest.port", 18081);
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
        /*checkpointConfig.setCheckpointStorage(
                Objects.requireNonNull(
                        FusionTest.class.getResource("/")).toString());*/

        final Configuration config = tool.getConfiguration();
        env.getConfig().setGlobalJobParameters(config);
        env.setParallelism(8);

        final DataStreamSource<ObjectNode> sourceStream = env.addSource(new DataGenSource());

        // Process the data using the TypeSplitFunction and split it into separate DataStreams for SIP and RTP data
        final SingleOutputStreamOperator<ObjectNode> splitsRecordsOperator =
                sourceStream
                        .name("DataGenSource")
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

        final KeySelector<ObjectNode, Integer> vSysSelector = new VSysIDKeySelector();

        // Fusion SIP data and RTP data to VoIP data.
        final SingleOutputStreamOperator<ObjectNode> voIpOperator = rtpDataStream
                .keyBy(vSysSelector)
                .connect(sipDoubleDirOperator.keyBy(vSysSelector))
                .process(new VoIPFusionFunction())
                .name("VoIPFusion")
                .uid("voip-fusion");

        voIpOperator.print();

        env.execute("VoIP Fusion Job");
    }
}
