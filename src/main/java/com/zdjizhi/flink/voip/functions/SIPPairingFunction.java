package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.conf.FusionConfigs;
import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.StreamDir;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class SIPPairingFunction extends KeyedProcessFunction<Tuple2<Integer, String>, ObjectNode, ObjectNode>
        implements FunctionHelper {

    private transient Time fireInterval;
    private transient MapState<Address, ObjectNodeInfo> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        fireInterval = Time.milliseconds(getGlobalConfiguration()
                .get(FusionConfigs.SIP_STATE_CLEAR_INTERVAL));
        mapState = getMapState("sip-state", Address.class, ObjectNodeInfo.class);
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
            if (mapState.contains(address) && new Record(mapState.get(address).getObj()).getStreamDir() != record.getStreamDir() /* TODO consider stream direction */) {
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
        registerNextFireTimestamp(ctx.timerService(), fireInterval);
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<Tuple2<Integer, String>, ObjectNode, ObjectNode>.OnTimerContext ctx,
                        Collector<ObjectNode> out) throws Exception {
        mapState.iterator().forEachRemaining(entry -> {
            if (entry.getValue().getExpireTime() <= timestamp) {
                try {
                    // Remove expired entries from the mapState based on their expiration time.
                    mapState.remove(entry.getKey());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        registerNextFireTimestamp(ctx.timerService(), fireInterval);
    }
}
