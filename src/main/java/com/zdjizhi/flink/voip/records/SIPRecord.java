package com.zdjizhi.flink.voip.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * SIP（Session Initiation Protocol）data record class, used to parse and access SIP data records.
 *
 * @author chaoc
 * @since 1.0
 */
public class SIPRecord extends Record {

    /**
     * Field Name: SIP 通话的会话 ID
     */
    public static final String F_CALL_ID = "sip_call_id";
    /**
     * Field Name: SIP 通话的协调的主叫语音传输 IP
     */
    public static final String F_ORIGINATOR_SDP_CONNECT_IP = "sip_originator_sdp_connect_ip";
    /**
     * Field Name: SIP 通话的协调的主叫语音传输端口
     */
    public static final String F_ORIGINATOR_SDP_MEDIA_PORT = "sip_originator_sdp_media_port";
    /**
     * Field Name: SIP 通话的协调的被叫语音传输 IP
     */
    public static final String F_RESPONDER_SDP_CONNECT_IP = "sip_responder_sdp_connect_ip";
    /**
     * Field Name: SIP 通话的协调的被叫语音传输端口
     */
    public static final String F_RESPONDER_SDP_MEDIA_PORT = "sip_responder_sdp_media_port";

    public SIPRecord(final ObjectNode obj) {
        super(obj);
    }

    public String getCallID() {
        return Record.getString(obj, F_CALL_ID);
    }

    public String getOriginatorSdpConnectIp() {
        return Record.getString(obj, F_ORIGINATOR_SDP_CONNECT_IP);
    }

    public String getOriginatorSdpMediaPort() {
        return Record.getString(obj, F_ORIGINATOR_SDP_MEDIA_PORT);
    }

    public String getResponderSdpConnectIp() {
        return Record.getString(obj, F_RESPONDER_SDP_CONNECT_IP);
    }

    public String getResponderSdpMediaPort() {
        return Record.getString(obj, F_RESPONDER_SDP_MEDIA_PORT);
    }
}
