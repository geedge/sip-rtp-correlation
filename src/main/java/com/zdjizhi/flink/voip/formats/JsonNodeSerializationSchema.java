package com.zdjizhi.flink.voip.formats;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonNodeSerializationSchema implements SerializationSchema<ObjectNode> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(ObjectNode jsonNodes) {
        try {
            return mapper.writeValueAsBytes(jsonNodes);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
