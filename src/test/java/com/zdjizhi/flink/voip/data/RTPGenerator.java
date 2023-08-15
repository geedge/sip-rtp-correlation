package com.zdjizhi.flink.voip.data;

import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SIPRecord;
import com.zdjizhi.flink.voip.records.SchemaType;
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
        final String json = "{\n" +
                "  \"common_address_list\": \"42924-36682-172.17.201.16-172.17.200.50\",\n" +
                "  \"common_address_type\": 4,\n" +
                "  \"common_app_full_path\": \"rtp\",\n" +
                "  \"common_app_label\": \"rtp\",\n" +
                "  \"common_c2s_byte_num\": 0,\n" +
                "  \"common_c2s_pkt_num\": 0,\n" +
                "  \"common_client_ip\": \"172.17.201.16\",\n" +
                "  \"common_client_port\": 42924,\n" +
                "  \"common_con_duration_ms\": 1086,\n" +
                "  \"common_device_id\": \"unknown\",\n" +
                "  \"common_device_tag\": \"{\\\"tags\\\":[{\\\"tag\\\":\\\"device_group\\\",\\\"value\\\":\\\"group-xxg-tsgxSfcOs\\\"}]}\",\n" +
                "  \"common_direction\": 73,\n" +
                "  \"common_end_time\": 1689295970,\n" +
                "  \"common_flags\": 16401,\n" +
                "  \"common_flags_identify_info\": \"{\\\"Asymmetric\\\":5,\\\"Server is Local\\\":1,\\\"S2C\\\":1}\",\n" +
                "  \"common_l4_protocol\": \"IPv4_UDP\",\n" +
                "  \"common_out_dest_mac\": \"02:fc:08:dc:92:d7\",\n" +
                "  \"common_out_src_mac\": \"02:fc:08:dc:91:c3\",\n" +
                "  \"common_protocol_label\": \"ETHERNET.IPv4.UDP\",\n" +
                "  \"common_s2c_byte_num\": 7570,\n" +
                "  \"common_s2c_pkt_num\": 5,\n" +
                "  \"common_schema_type\": \"RTP\",\n" +
                "  \"common_server_ip\": \"172.17.200.50\",\n" +
                "  \"common_server_port\": 36682,\n" +
                "  \"common_sessions\": 1,\n" +
                "  \"common_sled_ip\": \"192.168.42.54\",\n" +
                "  \"common_start_time\": 1689294629,\n" +
                "  \"common_stream_dir\": 3,\n" +
                "  \"common_stream_trace_id\": \"290484792956466709\",\n" +
                "  \"common_t_vsys_id\": 24,\n" +
                "  \"common_vsys_id\": 24,\n" +
                "  \"raw_log_status\": \"CLOSE\",\n" +
                "  \"rtp_payload_type_c2s\": 0,\n" +
                "  \"rtp_payload_type_s2c\": 0,\n" +
                "  \"rtp_pcap_path\": \"http://192.168.44.67:9098/hos/rtp_hos_bucket/rtp_172.17.200.50_172.17.201.16_27988_55806_1689294629.pcap\"\n" +
                "}";

        ObjectNode obj;
        try {
            obj = mapper.readValue(json, ObjectNode.class);
        } catch (Exception e){
            obj = mapper.createObjectNode();
        }
        final SIPRecord record = new SIPRecord(obj);

        record.setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.RTP.getValue());
        record.setString(SIPRecord.F_CALL_ID, nextId());
        record.setInt(SIPRecord.F_COMMON_STREAM_DIR, random.nextInt(3) + 1);

        record.setString(Record.F_COMMON_SERVER_IP, nextIp());
        record.setInt(Record.F_COMMON_SERVER_PORT, nextPort());
        record.setString(Record.F_COMMON_CLIENT_IP, nextIp());
        record.setInt(Record.F_COMMON_CLIENT_PORT, nextPort());
        return obj;
    }

    @Override
    protected ObjectNode afterState(ObjectNode v) {
        final Record record = new Record(v);
        switch (record.getStreamDir()) {
            case DOUBLE:
                record.setInt(Record.F_COMMON_STREAM_DIR, random.nextInt(2) + 1);
                break;
            case S2C:
            default:
        }
        return v;
    }
}
