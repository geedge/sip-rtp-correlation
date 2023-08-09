package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.conf.FusionConfigs;
import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SIPRecord;
import com.zdjizhi.flink.voip.records.SchemaType;
import com.zdjizhi.flink.voip.records.StreamDir;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

/**
 * The `VoIPFusionFunction` is a key-based co-process function that performs VoIP fusion logic
 * for SIP and RTP records. It combines SIP and RTP records belonging to the same session
 * and emits fused VoIP records. The function utilizes keyed state to store and manage SIP and
 * RTP records, and it uses timers to trigger regular clearing of the state.
 *
 * @author chaoc
 * @since 1.0
 */
public class VoIPFusionFunction extends KeyedCoProcessFunction<Integer, ObjectNode, ObjectNode, ObjectNode>
        implements FunctionHelper {

    private static final int MAX_RTP_LINES = 2;
    private transient Time fireInterval;
    private transient MapState<Address, ObjectNodeInfo> sipDoubleDirState;
    private transient MapState<Address, ObjectNodeInfo> rtpState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        final int minutes = getGlobalConfiguration()
                .get(FusionConfigs.RTP_STATE_CLEAR_INTERVAL);
        fireInterval = Time.minutes(minutes);
        final RuntimeContext context = getRuntimeContext();
        final StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(fireInterval)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        final MapStateDescriptor<Address, ObjectNodeInfo> sipDescriptor =
                new MapStateDescriptor<>("sip-state", Address.class, ObjectNodeInfo.class);
        sipDescriptor.enableTimeToLive(ttlConfig);
        sipDoubleDirState = context.getMapState(sipDescriptor);

        final MapStateDescriptor<Address, ObjectNodeInfo> rtpDescriptor =
                new MapStateDescriptor<>("rtp-state", Address.class, ObjectNodeInfo.class);
        rtpDescriptor.enableTimeToLive(ttlConfig);
        rtpState = context.getMapState(rtpDescriptor);
    }

    @Override
    public void processElement2(ObjectNode obj,
                                KeyedCoProcessFunction<Integer, ObjectNode, ObjectNode, ObjectNode>.Context ctx,
                                Collector<ObjectNode> out) throws Exception {
        final SIPRecord record = new SIPRecord(obj);
        final Address address = Address.of(
                Tuple2.of(record.getOriginatorSdpConnectIp(), record.getOriginatorSdpMediaPort()),
                Tuple2.of(record.getResponderSdpConnectIp(), record.getResponderSdpMediaPort()));
        sipDoubleDirState.put(address, new ObjectNodeInfo(obj,
                ctx.timerService().currentProcessingTime() + fireInterval.toMilliseconds()));
        registerNextFireTimestamp(ctx.timerService(), ctx.getCurrentKey().hashCode());
    }

    @Override
    public void processElement1(ObjectNode obj,
                                KeyedCoProcessFunction<Integer, ObjectNode, ObjectNode, ObjectNode>.Context ctx,
                                Collector<ObjectNode> out) throws Exception {
        final Record record = new Record(obj);
        final Address address = Address.of(Tuple2.of(record.getServerIp(), record.getServerPort()),
                Tuple2.of(record.getClientIp(), record.getClientPort()));
        if (sipDoubleDirState.contains(address)) {
            final ObjectNodeInfo info = sipDoubleDirState.get(address);
            record.merge(info.getObj())
                    .setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.VOIP.getValue());
            out.collect(obj);
            // In the context of VoIP fusion, only one RTP double directional stream
            if (record.getStreamDir() == StreamDir.DOUBLE.getValue()) {
                sipDoubleDirState.remove(address);
            } else { // Save the number of fused RTP unidirectional streams
                info.setTimes(info.getTimes() + 1);
                sipDoubleDirState.put(address, info);
            }

        } else {
            rtpState.put(address, new ObjectNodeInfo(obj,
                    ctx.timerService().currentProcessingTime() + fireInterval.toMilliseconds()));
        }
        registerNextFireTimestamp(ctx.timerService(), ctx.getCurrentKey().hashCode());
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedCoProcessFunction<Integer, ObjectNode, ObjectNode, ObjectNode>.OnTimerContext ctx,
                        Collector<ObjectNode> out) throws Exception {
        final Iterator<Map.Entry<Address, ObjectNodeInfo>> iterator = rtpState.iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Address, ObjectNodeInfo> entry = iterator.next();
            final Address address = entry.getKey();
            final ObjectNode obj = entry.getValue().getObj();
            final Record record = new Record(obj);
            if (sipDoubleDirState.contains(address)) {
                final ObjectNodeInfo info = sipDoubleDirState.get(address);
                if (record.getStreamDir() == StreamDir.DOUBLE.getValue()
                        || info.getTimes() >= MAX_RTP_LINES - 1
                    /* One RTP unidirectional stream has already been fused*/) {
                    sipDoubleDirState.remove(address);
                }
                record.merge(info.getObj())
                        .setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.VOIP.getValue());
                out.collect(obj);
            }
            if (entry.getValue().getExpireTime() <= timestamp) {
                out.collect(entry.getValue().getObj());
                iterator.remove();
            }
        }
        final Iterator<Map.Entry<Address, ObjectNodeInfo>> sipIterator = sipDoubleDirState.iterator();
        while (sipIterator.hasNext()) {
            final Map.Entry<Address, ObjectNodeInfo> entry = sipIterator.next();
            if (entry.getValue().getExpireTime() <= timestamp) {
                sipIterator.remove();
            }
        }
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + INTERVAL);
    }
}