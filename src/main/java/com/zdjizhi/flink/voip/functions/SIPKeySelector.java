package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.records.SIPRecord;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A KeySelector implementation for extracting a composite key from an ObjectNode representing a SIP record.
 *
 * @author chaoc
 * @since 1.0
 */
public class SIPKeySelector implements KeySelector<ObjectNode, Tuple2<Integer, String>> {

    /**
     * Extracts the composite key (VSysID, CallID) from the given ObjectNode.
     *
     * @param obj The ObjectNode representing a SIP record.
     * @return A Tuple2 containing the extracted key (VSysID, CallID).
     * @throws Exception Thrown if an error occurs during key extraction.
     */
    @Override
    public Tuple2<Integer, String> getKey(ObjectNode obj) throws Exception {
        final SIPRecord record = new SIPRecord(obj);
        return Tuple2.of(record.getVSysID(), record.getCallID());
    }
}