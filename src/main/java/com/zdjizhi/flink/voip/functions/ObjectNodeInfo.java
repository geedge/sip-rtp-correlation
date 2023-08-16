package com.zdjizhi.flink.voip.functions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A case class representing an ObjectNode with an expiration time and a pair times.
 *
 * @author chaoc
 * @since 1.0
 */
@Data
@AllArgsConstructor
class ObjectNodeInfo {

    // The ObjectNode containing data.
    private ObjectNode obj;

    // The pair times for the object.
    @Setter
    private int times;

    public void incTimes() {
        this.times = this.times + 1;
    }
}
