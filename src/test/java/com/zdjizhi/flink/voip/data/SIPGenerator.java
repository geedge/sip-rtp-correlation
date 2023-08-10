package com.zdjizhi.flink.voip.data;

import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SIPRecord;
import com.zdjizhi.flink.voip.records.SchemaType;
import com.zdjizhi.flink.voip.records.StreamDir;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * SIPGenerator extends Generator<ObjectNode> and is responsible for generating SIP (Session Initiation Protocol) records.
 * It generates random SIP records with specific properties such as call ID, stream dir, server IP, server port,
 * client IP, client port... information for both originator and responder.
 *
 * @author chaoc
 * @since 1.0
 */
public class SIPGenerator extends Generator<ObjectNode> {
    public SIPGenerator(final int ratio) {
        super(ratio);
    }

    /**
     * Creates a new SIPGenerator with the default ratio of 40.
     */
    public SIPGenerator() {
        this(40);
    }

    @Override
    public final ObjectNode next() {
        int i = random.nextInt(100);
        if (i < ratio && state.get() != null) {
            ObjectNode t = afterState(state.get());
            state.set(null);
            return t;
        } else {
            return state.updateAndGet(t -> generate());
        }
    }


    @Override
    protected ObjectNode generate() {
        final ObjectNode obj = mapper.createObjectNode();

        final SIPRecord record = new SIPRecord(obj);

        record.setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.SIP.getValue());

        record.setString(SIPRecord.F_CALL_ID, nextId());
        record.setInt(SIPRecord.F_COMMON_STREAM_DIR, (random.nextBoolean() ? StreamDir.S2C : StreamDir.C2S).getValue());

        record.setString(Record.F_COMMON_SERVER_IP, nextIp());
        record.setInt(Record.F_COMMON_SERVER_PORT, nextPort());
        record.setString(Record.F_COMMON_CLIENT_IP, nextIp());
        record.setInt(Record.F_COMMON_CLIENT_PORT, nextPort());

        record.setString(SIPRecord.F_ORIGINATOR_SDP_CONNECT_IP, nextIp());
        record.setInt(SIPRecord.F_ORIGINATOR_SDP_MEDIA_PORT, nextPort());
        record.setString(SIPRecord.F_RESPONDER_SDP_CONNECT_IP, nextIp());
        record.setInt(SIPRecord.F_RESPONDER_SDP_MEDIA_PORT, nextPort());
        return obj;
    }

    @Override
    protected ObjectNode afterState(ObjectNode v) {
        final Record record = new Record(v);
        switch (StreamDir.of(record.getStreamDir())) {
            case C2S:
                record.setInt(Record.F_COMMON_STREAM_DIR, StreamDir.S2C.getValue());
                break;
            case S2C:
                record.setInt(Record.F_COMMON_STREAM_DIR, StreamDir.C2S.getValue());
                break;
            default:
        }
        return v;
    }
}
