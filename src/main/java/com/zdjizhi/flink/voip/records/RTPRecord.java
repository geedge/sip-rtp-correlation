package com.zdjizhi.flink.voip.records;

import lombok.Getter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class RTPRecord extends Record {

    public static final String F_ORIGINATOR_DIR = "rtp_originator_dir";

    public RTPRecord(ObjectNode obj) {
        super(obj);
    }

    @Getter
    public enum OriginatorDir {

        UNKNOWN(0),
        C2S(1),
        S2C(2);

        private final int code;

        OriginatorDir(int code) {
            this.code = code;
        }
    }
}
