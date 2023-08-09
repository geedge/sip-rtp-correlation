package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.conf.FusionConfigs;
import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.StreamDir;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

/**
 * A KeyedProcessFunction that pairs SIP records based on their addresses and stream direction.
 * SIP records are paired when they have the same addresses but opposite stream directions.
 *
 * @author chaoc
 * @since 1.0
 */
public class SIPPairingFunction extends KeyedProcessFunction<Tuple2<Integer, String>, ObjectNode, ObjectNode>
        implements FunctionHelper {

    private transient Time fireInterval;
    private transient MapState<Address, ObjectNodeInfo> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int minutes = getGlobalConfiguration().get(FusionConfigs.SIP_STATE_CLEAR_INTERVAL);
        fireInterval = Time.minutes(minutes);

        final MapStateDescriptor<Address, ObjectNodeInfo> descriptor =
                new MapStateDescriptor<>("sip-state", Address.class, ObjectNodeInfo.class);

        final StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(fireInterval)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
        descriptor.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(ObjectNode value,
                               KeyedProcessFunction<Tuple2<Integer, String>, ObjectNode, ObjectNode>.Context ctx,
                               Collector<ObjectNode> out) throws Exception {

        final Record record = new Record(value);
        // When SIP is a one-way stream.
        if (StreamDir.DOUBLE.getValue() != record.getStreamDir()) {
            // Create an Address instance based on server and client IPs and ports.
            final Address address = Address.of(Tuple2.of(record.getClientIp(), record.getClientPort()),
                    Tuple2.of(record.getServerIp(), record.getServerPort()));
            // If the address is already stored in the mapState and has the opposite stream direction,
            // merge the SIP records, change the stream direction to DOUBLE, and output the merged record.
            if (mapState.contains(address) &&
                    new Record(mapState.get(address).getObj()).getStreamDir() != record.getStreamDir()
                /* TODO consider stream direction */) {
                record.merge(mapState.get(address).getObj());
                record.setInt(Record.F_COMMON_STREAM_DIR, StreamDir.DOUBLE.getValue());
                out.collect(value);
                mapState.remove(address);
            } else {
                // If the address is not yet in the mapState, add it with its expiration time.
                final ObjectNodeInfo info = new ObjectNodeInfo(value,
                        ctx.timerService().currentProcessingTime() + fireInterval.toMilliseconds());
                mapState.put(address, info);
            }
        } else {
            // If SIP is a double stream, pairing isn't required, directly output the record.
            out.collect(value);
        }
        registerNextFireTimestamp(ctx.timerService(), ctx.getCurrentKey().hashCode());
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<Tuple2<Integer, String>, ObjectNode, ObjectNode>.OnTimerContext ctx,
                        Collector<ObjectNode> out) throws Exception {
        final Iterator<Map.Entry<Address, ObjectNodeInfo>> iterator = mapState.iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Address, ObjectNodeInfo> entry = iterator.next();
            if (entry.getValue().getExpireTime() <= timestamp) {
                iterator.remove();
            }
        }
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + INTERVAL);
    }
}
