package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.records.Record;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A KeySelector implementation that extracts the key(VSysID) from an ObjectNode.
 *
 * @author chaoc
 * @since 1.0
 */
public class VSysIDKeySelector implements KeySelector<ObjectNode, Integer> {

    /**
     * Gets the VSysID from the provided ObjectNode.
     *
     * @param obj The input ObjectNode.
     * @return The extracted key(VSysID).
     * @throws Exception Thrown if there is an error extracting the VSysID.
     */
    @Override
    public Integer getKey(ObjectNode obj) throws Exception {
        final Record record = new Record(obj);
        return record.getVSysID();
    }
}
