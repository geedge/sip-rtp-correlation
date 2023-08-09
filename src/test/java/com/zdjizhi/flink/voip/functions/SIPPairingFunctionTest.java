package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.conf.FusionConfigs;
import com.zdjizhi.flink.voip.data.SIPGenerator;
import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.StreamDir;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


public class SIPPairingFunctionTest {
    private static final int INTERVAL_MINUTES = 5;
    private static KeyedOneInputStreamOperatorTestHarness<Integer, ObjectNode, ObjectNode> testHarness;

    @BeforeAll
    static void setUp() throws Exception {
        final SIPPairingFunction func = new SIPPairingFunction();
        final KeyedProcessOperator<Tuple3<Integer, String, Address>, ObjectNode, ObjectNode> operator =
                new KeyedProcessOperator<>(func);
        final TypeInformation<Integer> type = TypeInformation.of(Integer.class);
        final KeySelector<ObjectNode, Integer> keySelector = jsonNodes -> 0;
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, type);

        final Configuration configuration = new Configuration();
        configuration.set(FusionConfigs.SIP_STATE_CLEAR_INTERVAL, INTERVAL_MINUTES);
        testHarness.getExecutionConfig().setGlobalJobParameters(configuration);
        testHarness.open();
    }

    @Test
    public void testProperlyPairSIPRecords() throws Exception {
        final SIPGenerator sipGenerator = new SIPGenerator();
        long current = System.currentTimeMillis();

        final ObjectNode obj = sipGenerator.next();
        final Record record = new Record(obj);
        record.setInt(Record.F_COMMON_STREAM_DIR, StreamDir.DOUBLE.getValue());

        final StreamRecord<ObjectNode> streamRecord = new StreamRecord<>(obj, current);

        testHarness.processElement(streamRecord);
        assertTrue(testHarness.getOutput().contains(streamRecord));

        long interval = Time.minutes(INTERVAL_MINUTES).toMilliseconds();
        long nextFireTime = (testHarness.getProcessingTime() / interval + 1) * interval;
        assertTrue(testHarness.getProcessingTimeService().getActiveTimerTimestamps().contains(nextFireTime));

        final ObjectNode obj1 = obj.deepCopy();
        final Record record1 = new Record(obj1);
        record1.setInt(Record.F_COMMON_STREAM_DIR, StreamDir.S2C.getValue());

        final ObjectNode obj2 = obj.deepCopy();
        final Record record2 = new Record(obj2);
        record2.setInt(Record.F_COMMON_STREAM_DIR, StreamDir.C2S.getValue());

        final MapStateDescriptor<Address, ObjectNodeInfo> descriptor =
                new MapStateDescriptor<>(
                        "sip-state",
                        TypeInformation.of(Address.class),
                        TypeInformation.of(ObjectNodeInfo.class)
                );

        testHarness.processElement(obj1, current);
        final MapState<Address, ObjectNodeInfo> mapState = testHarness.getOperator()
                .getKeyedStateStore().getMapState(descriptor);
        final Set<ObjectNode> objectNodes1 = Lists.newArrayList(mapState.values()).stream()
                .map(ObjectNodeInfo::getObj)
                .collect(Collectors.toSet());
        assertTrue(objectNodes1.contains(obj1));

        testHarness.processElement(obj2, current);
        final Set<ObjectNode> objectNodes2 = Lists.newArrayList(mapState.values()).stream()
                .map(ObjectNodeInfo::getObj)
                .collect(Collectors.toSet());
        assertFalse(objectNodes2.contains(obj2));
        assertTrue(testHarness.getOutput().contains(new StreamRecord<>(obj2, current)));
        assertEquals(2, testHarness.getOutput().size());
    }


    @AfterAll
    static void close() throws Exception {
        testHarness.close();
    }

}