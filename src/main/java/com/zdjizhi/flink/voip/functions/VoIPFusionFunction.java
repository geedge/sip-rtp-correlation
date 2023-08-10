package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.conf.FusionConfigs;
import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SchemaType;
import com.zdjizhi.flink.voip.records.StreamDir;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * The `VoIPFusionFunction` is a key-based co-process function that performs VoIP fusion logic
 * for SIP and RTP records. It combines SIP and RTP records belonging to the same session
 * and emits fused VoIP records. The function utilizes keyed state to store and manage SIP and
 * RTP records, and it uses timers to trigger regular clearing of the state.
 *
 * @author chaoc
 * @since 1.0
 */
public class VoIPFusionFunction extends KeyedCoProcessFunction<Tuple2<Integer, Address>, ObjectNode, ObjectNode, ObjectNode>
        implements FunctionHelper {

    private static final int MAX_RTP_LINES = 2;
    private transient Time fireInterval;
    private transient ValueState<ObjectNodeInfo> sipDoubleDirState;
    private transient ValueState<ObjectNodeInfo> rtpState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        final int minutes = getGlobalConfiguration().get(FusionConfigs.RTP_STATE_CLEAR_INTERVAL);
        fireInterval = Time.minutes(minutes);
        final RuntimeContext context = getRuntimeContext();
        final ValueStateDescriptor<ObjectNodeInfo> sipDescriptor =
                new ValueStateDescriptor<>("sip-state", ObjectNodeInfo.class);

        final ValueStateDescriptor<ObjectNodeInfo> rtpDescriptor =
                new ValueStateDescriptor<>("rtp-state", ObjectNodeInfo.class);

        final StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(fireInterval)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .useProcessingTime()
                .cleanupFullSnapshot()
                .build();

        rtpDescriptor.enableTimeToLive(ttlConfig);
        sipDescriptor.enableTimeToLive(ttlConfig);

        rtpState = context.getState(rtpDescriptor);
        sipDoubleDirState = context.getState(sipDescriptor);
    }

    // SIP
    @Override
    public void processElement2(ObjectNode obj,
                                KeyedCoProcessFunction<Tuple2<Integer, Address>, ObjectNode, ObjectNode, ObjectNode>.Context ctx,
                                Collector<ObjectNode> out) throws Exception {
        final ObjectNodeInfo info = rtpState.value();
        if (info != null) {
            final Record record = new Record(info.getObj());
            record.merge(obj)
                    .setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.VOIP.getValue());
            out.collect(info.getObj());
            if (record.getStreamDir() != StreamDir.DOUBLE.getValue()) {
                sipDoubleDirState.update(new ObjectNodeInfo(obj,
                        ctx.timerService().currentProcessingTime() + fireInterval.toMilliseconds()));
            }
        } else {
            sipDoubleDirState.update(new ObjectNodeInfo(obj,
                    ctx.timerService().currentProcessingTime() + fireInterval.toMilliseconds()));
        }
        registerNextFireTimestamp(ctx.timerService(), ctx.getCurrentKey().hashCode());
    }

    // RTP
    @Override
    public void processElement1(ObjectNode obj,
                                KeyedCoProcessFunction<Tuple2<Integer, Address>, ObjectNode, ObjectNode, ObjectNode>.Context ctx,
                                Collector<ObjectNode> out) throws Exception {
        final Record record = new Record(obj);
        final ObjectNodeInfo info = sipDoubleDirState.value();
        if (null != info) {
            record.merge(info.getObj())
                    .setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.VOIP.getValue());
            out.collect(obj);
            // In the context of VoIP fusion, only one RTP double directional stream
            if (record.getStreamDir() == StreamDir.DOUBLE.getValue()
                    || info.getTimes() >= MAX_RTP_LINES - 1) {
                sipDoubleDirState.clear();
            } else { // Save the number of fused RTP unidirectional streams
                info.setTimes(info.getTimes() + 1);
                sipDoubleDirState.update(info);
            }

        } else {
            rtpState.update(new ObjectNodeInfo(obj,
                    ctx.timerService()
                            .currentProcessingTime() + fireInterval.toMilliseconds()));
        }
        registerNextFireTimestamp(ctx.timerService(), ctx.getCurrentKey().hashCode());
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedCoProcessFunction<Tuple2<Integer, Address>, ObjectNode, ObjectNode, ObjectNode>.OnTimerContext ctx,
                        Collector<ObjectNode> out) throws Exception {
        final ObjectNodeInfo rtpInfo = rtpState.value();
        if (rtpInfo != null && rtpInfo.getExpireTime() <= timestamp) {
            out.collect(rtpInfo.getObj());
            rtpState.clear();
        }
        final ObjectNodeInfo sipInfo = sipDoubleDirState.value();
        if (sipInfo != null && sipInfo.getExpireTime() <= timestamp) {
            sipDoubleDirState.clear();
        }
        if (rtpInfo != null || sipInfo != null) {
            registerIntervalFireTimestamp(ctx.timerService());
        }
    }
}