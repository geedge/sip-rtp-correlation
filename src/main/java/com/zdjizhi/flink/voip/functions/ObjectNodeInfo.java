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
    private final ObjectNode obj;

    // The expiration time for the object.
    private final long expireTime;

    // The pair times for the object.
    @Setter
    private int times;

    public ObjectNodeInfo(final ObjectNode obj, final long expireTime) {
        this.obj = obj;
        this.expireTime = expireTime;
        this.times = 0;
    }
}
