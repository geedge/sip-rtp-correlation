package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.conf.FusionConfigs;
import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SchemaType;
import com.zdjizhi.flink.voip.records.StreamDir;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
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
public class VoIPFusionFunction extends KeyedCoProcessFunction<Tuple2<Integer, Address>, ObjectNode, ObjectNode, ObjectNode>
        implements FunctionHelper {

    private static final int MAX_RTP_LINES = 2;
    private transient Time fireInterval;
    private transient ValueState<ObjectNodeInfo> sipState;
    private transient MapState<StreamDir, ObjectNode> rtpState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        final int minutes = getGlobalConfiguration().get(FusionConfigs.RTP_STATE_CLEAR_INTERVAL);
        fireInterval = Time.minutes(minutes);
        final RuntimeContext context = getRuntimeContext();
        final ValueStateDescriptor<ObjectNodeInfo> sipDescriptor =
                new ValueStateDescriptor<>("sip-state", ObjectNodeInfo.class);

        final StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(fireInterval)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .useProcessingTime()
                .cleanupFullSnapshot()
                .build();
        sipDescriptor.enableTimeToLive(ttlConfig);
        sipState = context.getState(sipDescriptor);

        final MapStateDescriptor<StreamDir, ObjectNode> rtpDescriptor =
                new MapStateDescriptor<>("rtp-state", StreamDir.class, ObjectNode.class);

        final StateTtlConfig rtpTtlConfig = StateTtlConfig
                .newBuilder(Time.minutes(minutes + 1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .useProcessingTime()
                .cleanupFullSnapshot()
                .build();
        rtpDescriptor.enableTimeToLive(rtpTtlConfig);
        rtpState = context.getMapState(rtpDescriptor);
    }

    // SIP
    @Override
    public void processElement2(ObjectNode sipObj,
                                KeyedCoProcessFunction<Tuple2<Integer, Address>, ObjectNode, ObjectNode, ObjectNode>.Context ctx,
                                Collector<ObjectNode> out) throws Exception {
        final Iterator<Map.Entry<StreamDir, ObjectNode>> iterator = rtpState.iterator();
        if (rtpState.isEmpty()) {
            sipState.update(new ObjectNodeInfo(sipObj, 0));
        }
        while (iterator.hasNext()) {
            final Map.Entry<StreamDir, ObjectNode> entry = iterator.next();
            final ObjectNode rtpObj = entry.getValue();
            final Record rtpRecord = new Record(rtpObj);

            rtpRecord.merge(sipObj)
                    .setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.VOIP.getValue());
            out.collect(rtpObj);
            iterator.remove();

            switch (entry.getKey()) {
                case S2C:
                case C2S:
                    ObjectNodeInfo info = sipState.value();
                    if (info != null) {
                        info.incTimes();
                        if (info.getTimes() >= MAX_RTP_LINES) {
                            sipState.clear();
                        } else {
                            sipState.update(new ObjectNodeInfo(sipObj, info.getTimes()));
                        }
                    } else {
                        sipState.update(new ObjectNodeInfo(sipObj, 1));
                    }
                    break;
                default:
                    // Double directional:
                    // In the context of VoIP fusion, only one RTP double directional stream
                    sipState.clear();
            }
        }

        registerNextFireTimestamp(ctx.timerService(), fireInterval.toMilliseconds());
    }

    // RTP
    @Override
    public void processElement1(ObjectNode rtpObj,
                                KeyedCoProcessFunction<Tuple2<Integer, Address>, ObjectNode, ObjectNode, ObjectNode>.Context ctx,
                                Collector<ObjectNode> out) throws Exception {
        final Record rtpRecord = new Record(rtpObj);
        final ObjectNodeInfo info = sipState.value();

        final StreamDir streamDir = rtpRecord.getStreamDir();
        if (null != info) {

            rtpRecord.merge(info.getObj())
                    .setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.VOIP.getValue());
            out.collect(rtpObj);

            switch (streamDir) {
                case C2S:
                case S2C:
                    info.incTimes();
                    if (info.getTimes() >= MAX_RTP_LINES) {
                        sipState.clear();
                    }
                    break;
                default:
                    // Double
                    sipState.clear();
            }

        } else {
            rtpState.put(streamDir, rtpObj);
        }

        registerNextFireTimestamp(ctx.timerService(), fireInterval.toMilliseconds());
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedCoProcessFunction<Tuple2<Integer, Address>, ObjectNode, ObjectNode, ObjectNode>.OnTimerContext ctx,
                        Collector<ObjectNode> out) throws Exception {
        for (ObjectNode obj : rtpState.values()) {
            out.collect(obj);
        }
        rtpState.clear();
        sipState.clear();
    }
}