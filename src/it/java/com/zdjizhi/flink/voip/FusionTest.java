package com.zdjizhi.flink.voip;

import com.zdjizhi.flink.voip.functions.DataGenSource;
import com.zdjizhi.flink.voip.functions.SIPPairingFunction;
import com.zdjizhi.flink.voip.functions.TypeSplitFunction;
import com.zdjizhi.flink.voip.functions.VoIPFusionFunction;
import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SIPRecord;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.ttl.mock.MockStateBackend;
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
        envConfig.setInteger("rest.port", 8081);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(envConfig);

        final ParameterTool tool = ParameterTool.fromPropertiesFile(
                FusionTest.class.getResourceAsStream("/application-test.properties"));

        final CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        env.enableCheckpointing(Time.minutes(1)
                .toMilliseconds(), CheckpointingMode.AT_LEAST_ONCE);
        checkpointConfig.setCheckpointTimeout(Time.minutes(1).toMilliseconds());
        checkpointConfig.setCheckpointStorage(
                Objects.requireNonNull(FusionTest.class.getResource("/")).toString());

        final Configuration config = tool.getConfiguration();
        env.getConfig().setGlobalJobParameters(config);
        env.setParallelism(1);

        final DataStreamSource<ObjectNode> sourceStream = env.addSource(new DataGenSource());

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

        final KeySelector<ObjectNode, Tuple2<Integer, String>> sipKeySelector = new KeySelector<>() {
            @Override
            public Tuple2<Integer, String> getKey(ObjectNode obj) throws Exception {
                final SIPRecord record = new SIPRecord(obj);
                return Tuple2.of(record.getVSysID(), record.getCallID());
            }
        };
        // Process SIP data to create a double directional stream using SIPPairingFunction.
        final SingleOutputStreamOperator<ObjectNode> sipDoubleDirOperator = sipDataStream
                .keyBy(sipKeySelector)
                .process(new SIPPairingFunction())
                .name("PairingOneWayToDoubleStream")
                .uid("pairing-one-way-to-double");

        final KeySelector<ObjectNode, Integer> vSysSelector = obj -> {
            final Record record = new Record(obj);
            return record.getVSysID();
        };

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
