package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.conf.FusionConfigs;
import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.StreamDir;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * A KeyedProcessFunction that pairs SIP records based on their addresses and stream direction.
 * SIP records are paired when they have the same addresses but opposite stream directions.
 *
 * @author chaoc
 * @since 1.0
 */
public class SIPPairingFunction extends KeyedProcessFunction<Tuple3<Integer, String, Address>, ObjectNode, ObjectNode>
        implements FunctionHelper {

    private transient Time fireInterval;

    private transient ValueState<ObjectNode> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int minutes = getGlobalConfiguration().get(FusionConfigs.SIP_STATE_CLEAR_INTERVAL);
        fireInterval = Time.minutes(minutes);
        final ValueStateDescriptor<ObjectNode> descriptor =
                new ValueStateDescriptor<>("sip-state", ObjectNode.class);

        final StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(fireInterval)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .useProcessingTime()
                .cleanupFullSnapshot()
                .build();
        descriptor.enableTimeToLive(ttlConfig);
        valueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(ObjectNode value,
                               KeyedProcessFunction<Tuple3<Integer, String, Address>, ObjectNode, ObjectNode>.Context ctx,
                               Collector<ObjectNode> out) throws Exception {

        final Record record = new Record(value);
        // When SIP is a one-way stream.
        if (StreamDir.DOUBLE != record.getStreamDir()) {
            // If the address is already stored in the mapState and has the opposite stream direction,
            // merge the SIP records, change the stream direction to DOUBLE, and output the merged record.
            final ObjectNode obj = valueState.value();
            if (null != obj && new Record(obj).getStreamDir() != record.getStreamDir()) {
                record.merge(obj)
                        .setInt(Record.F_COMMON_STREAM_DIR, StreamDir.DOUBLE.getValue());
                out.collect(value);
                valueState.clear();
            } else {
                // If the address is not yet in the mapState.
                valueState.update(value);
            }
        } else {
            // If SIP is a double stream, pairing isn't required, directly output the record.
            out.collect(value);
        }
        registerNextFireTimestamp(ctx.timerService(), fireInterval.toMilliseconds());
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<Tuple3<Integer, String, Address>, ObjectNode, ObjectNode>.OnTimerContext ctx,
                        Collector<ObjectNode> out) throws Exception {
        valueState.clear();
    }
}
