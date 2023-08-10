package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SIPRecord;
import com.zdjizhi.flink.voip.records.SchemaType;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * A KeySelector implementation that extracts the key(VSysID) from an ObjectNode.
 *
 * @author chaoc
 * @since 1.0
 */
public class VSysIDKeySelector implements KeySelector<ObjectNode, Tuple2<Integer, Address>> {

    /**
     * Extracts the composite key (VSysID, Address) from the given ObjectNode.
     *
     * @param obj The ObjectNode representing a SIP record.
     * @return A Tuple2 containing the extracted key (VSysID, Address).
     * @throws Exception Thrown if an error occurs during key extraction.
     */
    @Override
    public Tuple2<Integer, Address> getKey(ObjectNode obj) throws Exception {
        final Record record = new Record(obj);
        final Address address;
        if (Objects.equals(record.getSchemaType(), SchemaType.SIP.getValue())) {
            final SIPRecord sipRecord = new SIPRecord(obj);
            address = Address.of(
                    Tuple2.of(sipRecord.getOriginatorSdpConnectIp(), sipRecord.getOriginatorSdpMediaPort()),
                    Tuple2.of(sipRecord.getResponderSdpConnectIp(), sipRecord.getResponderSdpMediaPort()));
        } else {
            address = Address.of(Tuple2.of(record.getServerIp(), record.getServerPort()),
                    Tuple2.of(record.getClientIp(), record.getClientPort()));
        }
        return Tuple2.of(record.getVSysID(), address);
    }
}
