package com.zdjizhi.flink.voip.records;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The SchemaType enum represents different types of data schemas.
 *
 * @author chaoc
 * @since 1.0
 */
@AllArgsConstructor
@Getter
public enum SchemaType {

    /**
     * Represents the SIP schema type.
     */
    SIP("SIP"),

    /**
     * Represents the RTP schema type.
     */
    RTP("RTP"),

    /**
     * Represents the VoIP schema type.
     */
    VOIP("VoIP");

    /**
     * The string value of the SchemaType.
     */
    private final String value;

    /**
     * Get the SchemaType enum based on the provided string value.
     *
     * @param value The string value of the SchemaType to retrieve.
     * @return The corresponding SchemaType enum.
     * @throws IllegalArgumentException if the provided value does not match any known SchemaType.
     */
    public static SchemaType of(final String value) {
        for (SchemaType schemaType : values()) {
            if (schemaType.value.equalsIgnoreCase(value)) {
                return schemaType;
            }
        }
        throw new IllegalArgumentException("Unknown SchemaType value '" + value + "'.");
    }
}