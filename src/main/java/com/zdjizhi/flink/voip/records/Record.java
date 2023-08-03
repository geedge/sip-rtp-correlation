package com.zdjizhi.flink.voip.records;

import lombok.AllArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.IntNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

/**
 * Record class represents a data record with various fields.
 * <p>
 * It provides getter and setter methods for accessing the fields of the data record.
 *
 * @author chaoc
 * @since 1.0
 */
@AllArgsConstructor
public class Record {

    /**
     * 字段名：数据记录中的所属 vsys
     */
    public static final String F_COMMON_VSYS_ID = "common_vsys_id";
    /**
     * 字段名：数据记录中的字段类型
     */
    public static final String F_COMMON_SCHEMA_TYPE = "common_schema_type";
    /**
     * 字段名：数据记录中的流类型
     */
    public static final String F_COMMON_STREAM_DIR = "common_stream_dir";
    /**
     * 字段名：数据记录中的服务端地址
     */
    public static final String F_COMMON_SERVER_IP = "common_server_ip";
    /**
     * 字段名：数据记录中的服务端端口
     */
    public static final String F_COMMON_SERVER_PORT = "common_server_port";
    /**
     * 字段名：数据记录中的客户端地址
     */
    public static final String F_COMMON_CLIENT_IP = "common_client_ip";
    /**
     * 字段名：数据记录中的客户端端口
     */
    public static final String F_COMMON_CLIENT_PORT = "common_client_port";

    /**
     * ObjectNode data.
     */
    protected final ObjectNode obj;

    /**
     * Get the VSys ID from the data record.
     *
     * @return The VSys ID as an integer.
     */
    public int getVSysID() {
        return Record.getInt(obj, F_COMMON_VSYS_ID);
    }

    /**
     * Get the schema type from the data record.
     *
     * @return The schema type as a string.
     */
    public final String getSchemaType() {
        return Record.getString(obj, F_COMMON_SCHEMA_TYPE);
    }

    /**
     * Get the stream direction from the data record.
     *
     * @return The stream direction as an integer.
     */
    public final int getStreamDir() {
        return Record.getInt(obj, F_COMMON_STREAM_DIR);
    }

    /**
     * Get the server IP address from the data record.
     *
     * @return The server IP address as a string.
     */
    public final String getServerIp() {
        return Record.getString(obj, F_COMMON_SERVER_IP);
    }

    /**
     * Get the server port from the data record.
     *
     * @return The server port as an integer.
     */
    public final int getServerPort() {
        return Record.getInt(obj, F_COMMON_SERVER_PORT);
    }

    /**
     * Get the client IP address from the data record.
     *
     * @return The client IP address as a string.
     */
    public final String getClientIp() {
        return Record.getString(obj, F_COMMON_CLIENT_IP);
    }

    /**
     * Get the client port from the data record.
     *
     * @return The client port as an integer.
     */
    public final int getClientPort() {
        return Record.getInt(obj, F_COMMON_CLIENT_PORT);
    }

    /**
     * Set an integer value to the specified field in the data record.
     *
     * @param name  The name of the field.
     * @param value The integer value to set.
     */
    public final void setInt(final String name, final int value) {
        obj.set(name, IntNode.valueOf(value));
    }

    /**
     * Set a string value to the specified field in the data record.
     *
     * @param name  The name of the field.
     * @param value The string value to set.
     */
    public final void setString(final String name, final String value) {
        obj.set(name, TextNode.valueOf(value));
    }

    /**
     * Merge the fields of another ObjectNode into the current data record.
     *
     * @param other The ObjectNode containing the fields to be merged.
     * @return The merged ObjectNode.
     */
    public final ObjectNode merge(final ObjectNode other) {
        other.fields().forEachRemaining(entry -> obj.set(entry.getKey(), entry.getValue()));
        return obj;
    }

    /**
     * Get an integer value from the specified field in the ObjectNode.
     *
     * @param obj          The ObjectNode to get the value from.
     * @param field        The name of the field.
     * @param defaultValue The default value to return if the field is not found or is not an integer.
     * @return The integer value from the field or the default value if the field is not found or is not an integer.
     */
    public static int getInt(final ObjectNode obj, final String field, final int defaultValue) {
        final JsonNode node = obj.get(field);
        return node != null && node.isInt() ? node.asInt(defaultValue) : defaultValue;
    }

    /**
     * Get an integer value from the specified field in the ObjectNode.
     *
     * @param obj   The ObjectNode to get the value from.
     * @param field The name of the field.
     * @return The integer value from the field or 0 if the field is not found or is not an integer.
     */
    public static int getInt(final ObjectNode obj, final String field) {
        return getInt(obj, field, 0);
    }

    /**
     * Get a string value from the specified field in the ObjectNode.
     *
     * @param obj          The ObjectNode to get the value from.
     * @param field        The name of the field.
     * @param defaultValue The default value to return if the field is not found or is not a string.
     * @return The string value from the field or the default value if the field is not found or is not a string.
     */
    public static String getString(final ObjectNode obj, final String field, final String defaultValue) {
        final JsonNode node = obj.get(field);
        return node != null && node.isTextual() ? node.asText(defaultValue) : defaultValue;
    }

    /**
     * Get a string value from the specified field in the ObjectNode.
     *
     * @param obj   The ObjectNode to get the value from.
     * @param field The name of the field.
     * @return The string value from the field or null if the field is not found or is not a string.
     */
    public static String getString(final ObjectNode obj, final String field) {
        return getString(obj, field, null);
    }
}