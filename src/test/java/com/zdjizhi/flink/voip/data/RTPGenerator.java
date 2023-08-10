package com.zdjizhi.flink.voip.data;

import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SIPRecord;
import com.zdjizhi.flink.voip.records.SchemaType;
import com.zdjizhi.flink.voip.records.StreamDir;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * RTPGenerator extends Generator<ObjectNode> and is responsible for generating RTP records.
 * It generates random RTP records with specific properties.
 *
 * @author chaoc
 * @since 1.0
 */
public class RTPGenerator extends Generator<ObjectNode> {
    private final SIPGenerator sipGenerator;

    public RTPGenerator(int ratio, SIPGenerator sipGenerator) {
        super(ratio);
        this.sipGenerator = sipGenerator;
    }

    @Override
    public ObjectNode next() {
        int i = random.nextInt(100);
        if (i < ratio) {
            final ObjectNode node = sipGenerator.state.get();
            if (null != node) {
                final ObjectNode obj = generate();
                obj.set(Record.F_COMMON_CLIENT_IP, node.get(SIPRecord.F_ORIGINATOR_SDP_CONNECT_IP));
                obj.set(Record.F_COMMON_CLIENT_PORT, node.get(SIPRecord.F_ORIGINATOR_SDP_MEDIA_PORT));
                obj.set(Record.F_COMMON_SERVER_IP, node.get(SIPRecord.F_RESPONDER_SDP_CONNECT_IP));
                obj.set(Record.F_COMMON_SERVER_PORT, node.get(SIPRecord.F_RESPONDER_SDP_MEDIA_PORT));
                return obj;
            }
        }
        return generate();
    }

    @Override
    protected ObjectNode generate() {
        final ObjectNode obj = mapper.createObjectNode();
        final SIPRecord record = new SIPRecord(obj);

        record.setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.RTP.getValue());
        record.setString(SIPRecord.F_CALL_ID, nextId());
        record.setInt(SIPRecord.F_COMMON_STREAM_DIR, random.nextInt(4));

        record.setString(Record.F_COMMON_SERVER_IP, nextIp());
        record.setInt(Record.F_COMMON_SERVER_PORT, nextPort());
        record.setString(Record.F_COMMON_CLIENT_IP, nextIp());
        record.setInt(Record.F_COMMON_CLIENT_PORT, nextPort());
        return obj;
    }

    @Override
    protected ObjectNode afterState(ObjectNode v) {
        final Record record = new Record(v);
        switch (StreamDir.of(record.getStreamDir())) {
            case DOUBLE:
                record.setInt(Record.F_COMMON_STREAM_DIR, random.nextInt(2) + 1);
                break;
            case S2C:
            default:
        }
        return v;
    }
}
