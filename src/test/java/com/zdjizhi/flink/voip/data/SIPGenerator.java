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
        final String json = "{\n" +
                "  \"common_schema_type\": \"SIP\",\n" +
                "  \"common_sessions\": 1,\n" +
                "  \"sip_call_id\": \"3257b5c0f3d0266@spirent.com\",\n" +
                "  \"sip_originator_description\": \"<sip:caller@spirent.com>\",\n" +
                "  \"sip_responder_description\": \"sip:callee@spirent.com\",\n" +
                "  \"sip_originator_sdp_content\": \"v=0\\r\\no=SIP-UA 3395000 3397200 IN IP4 172.17.200.50\\r\\ns=SIP Call\\r\\nc=IN IP4 172.17.200.50\\r\\nt=0 0\\r\\nm=audio 36682 RTP/AVP 0\\r\\na=rtpmap:0 PCMU/8000\\r\\n\",\n" +
                "  \"sip_originator_sdp_connect_ip\": \"172.17.200.50\",\n" +
                "  \"sip_originator_sdp_media_port\": 36682,\n" +
                "  \"sip_originator_sdp_media_type\": \"0 PCMU/8000\",\n" +
                "  \"common_first_ttl\": 128,\n" +
                "  \"common_c2s_ipfrag_num\": 0,\n" +
                "  \"common_s2c_ipfrag_num\": 0,\n" +
                "  \"common_c2s_tcp_unorder_num\": 0,\n" +
                "  \"common_s2c_tcp_unorder_num\": 0,\n" +
                "  \"common_c2s_tcp_lostlen\": 0,\n" +
                "  \"common_s2c_tcp_lostlen\": 0,\n" +
                "  \"common_c2s_pkt_retrans\": 2,\n" +
                "  \"common_s2c_pkt_retrans\": 0,\n" +
                "  \"common_c2s_byte_retrans\": 1100,\n" +
                "  \"common_s2c_byte_retrans\": 0,\n" +
                "  \"common_direction\": 69,\n" +
                "  \"common_app_full_path\": \"sip\",\n" +
                "  \"common_app_label\": \"sip\",\n" +
                "  \"common_tcp_client_isn\": 3004427198,\n" +
                "  \"common_server_ip\": \"172.17.201.16\",\n" +
                "  \"common_client_ip\": \"172.17.200.49\",\n" +
                "  \"common_server_port\": 5060,\n" +
                "  \"common_client_port\": 6948,\n" +
                "  \"common_stream_dir\": 1,\n" +
                "  \"common_address_type\": 4,\n" +
                "  \"common_address_list\": \"6948-5060-172.17.200.50-172.17.201.16\",\n" +
                "  \"common_start_time\": 1689295655,\n" +
                "  \"common_end_time\": 1689295670,\n" +
                "  \"common_con_duration_ms\": 41467,\n" +
                "  \"common_s2c_pkt_num\": 0,\n" +
                "  \"common_s2c_byte_num\": 0,\n" +
                "  \"common_c2s_pkt_num\": 6,\n" +
                "  \"common_c2s_byte_num\": 1834,\n" +
                "  \"common_establish_latency_ms\": 249,\n" +
                "  \"common_out_src_mac\": \"02:fc:08:dc:91:c3\",\n" +
                "  \"common_out_dest_mac\": \"02:fc:08:dc:92:d7\",\n" +
                "  \"common_flags\": 8201,\n" +
                "  \"common_flags_identify_info\": \"{\\\"Asymmetric\\\":6,\\\"Client is Local\\\":1,\\\"C2S\\\":1}\",\n" +
                "  \"common_protocol_label\": \"ETHERNET.IPv4.TCP\",\n" +
                "  \"common_stream_trace_id\": \"290502385134123507\",\n" +
                "  \"common_l4_protocol\": \"IPv4_TCP\",\n" +
                "  \"common_sled_ip\": \"192.168.42.54\",\n" +
                "  \"common_device_id\": \"unknown\",\n" +
                "  \"common_device_tag\": \"{\\\"tags\\\":[{\\\"tag\\\":\\\"device_group\\\",\\\"value\\\":\\\"group-xxg-tsgxSfcOs\\\"}]}\",\n" +
                "  \"common_t_vsys_id\": 24,\n" +
                "  \"common_vsys_id\": 24\n" +
                "}";

        ObjectNode obj;
        try {
            obj = mapper.readValue(json, ObjectNode.class);
        } catch (Exception e) {
            obj = mapper.createObjectNode();
        }

        final SIPRecord record = new SIPRecord(obj);

        record.setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.SIP.getValue());

        record.setString(SIPRecord.F_CALL_ID, nextId() + "@spirent.com");
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
        switch (record.getStreamDir()) {
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
