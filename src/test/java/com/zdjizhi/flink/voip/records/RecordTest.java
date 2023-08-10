package com.zdjizhi.flink.voip.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.IntNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordTest {

    private static ObjectMapper mapper;

    @BeforeAll
    static void setUp() {
        mapper = new ObjectMapper();
    }

    @Test
    void testGetVSysID() {
        final ObjectNode obj = mapper.createObjectNode();
        final Record record = new Record(obj);
        record.setInt(Record.F_COMMON_VSYS_ID, 42);
        assertEquals(42, record.getVSysID());

        obj.set(Record.F_COMMON_VSYS_ID, IntNode.valueOf(40));
        assertEquals(40, record.getVSysID());
    }

    @Test
    void testGetSchemaType() {
        final ObjectNode obj = mapper.createObjectNode();
        final Record record = new Record(obj);
        record.setString(Record.F_COMMON_SCHEMA_TYPE, SchemaType.RTP.getValue());
        assertEquals(SchemaType.RTP.getValue(), record.getSchemaType());

        obj.set(Record.F_COMMON_SCHEMA_TYPE, TextNode.valueOf(SchemaType.VOIP.getValue()));
        assertEquals(SchemaType.VOIP.getValue(), record.getSchemaType());
    }

    @Test
    void testSetInt() {
        final ObjectNode obj = mapper.createObjectNode();
        final Record record = new Record(obj);
        record.setInt("intField", 123);
        assertEquals(123, obj.get("intField").intValue());
    }

    @Test
    void testSetString() {
        final ObjectNode obj = mapper.createObjectNode();
        final Record record = new Record(obj);
        record.setString("stringField", "testValue");
        assertEquals("testValue", obj.get("stringField").textValue());
    }

    @Test
    void testMerge() {
        final ObjectNode obj = mapper.createObjectNode();
        obj.set("field1", IntNode.valueOf(1));
        ObjectNode other = mapper.createObjectNode();
        other.set("field2", TextNode.valueOf("value2"));
        Record record = new Record(obj);
        record.merge(other);
        assertEquals(1, obj.get("field1").intValue());
        assertEquals("value2", obj.get("field2").textValue());
    }
}
