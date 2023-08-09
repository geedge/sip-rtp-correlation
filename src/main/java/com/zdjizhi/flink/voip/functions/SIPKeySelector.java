package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.records.SIPRecord;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A KeySelector implementation for extracting a composite key from an ObjectNode representing a SIP record.
 *
 * @author chaoc
 * @since 1.0
 */
public class SIPKeySelector implements KeySelector<ObjectNode, Tuple3<Integer, String, Address>> {

    /**
     * Extracts the composite key (VSysID, CallID, Address) from the given ObjectNode.
     *
     * @param obj The ObjectNode representing a SIP record.
     * @return A Tuple3 containing the extracted key (VSysID, CallID, Address).
     * @throws Exception Thrown if an error occurs during key extraction.
     */
    @Override
    public Tuple3<Integer, String, Address> getKey(ObjectNode obj) throws Exception {
        final SIPRecord record = new SIPRecord(obj);
        final Address address = Address.of(Tuple2.of(record.getClientIp(), record.getClientPort()),
                Tuple2.of(record.getServerIp(), record.getServerPort()));
        return Tuple3.of(record.getVSysID(), record.getCallID(), address);
    }
}