package com.zdjizhi.flink.voip.records;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The StreamDir enum represents different types of data stream directions.
 *
 * @author chaoc
 * @since 1.0
 */
@AllArgsConstructor
@Getter
public enum StreamDir {

    /**
     * Represents the Client-to-Server (C2S) stream direction.
     */
    C2S(1),

    /**
     * Represents the Server-to-Client (S2C) stream direction.
     */
    S2C(2),

    /**
     * Represents the bidirectional (double) stream direction.
     */
    DOUBLE(3);

    /**
     * The integer value of the StreamDir.
     */
    private final int value;

    /**
     * Get the StreamDir enum based on the provided integer value.
     *
     * @param value The integer value of the StreamDir to retrieve.
     * @return The corresponding StreamDir enum.
     * @throws IllegalArgumentException if the provided value does not match any known StreamDir.
     */
    public static StreamDir of(int value) {
        for (StreamDir streamDir : values()) {
            if (value == streamDir.value) {
                return streamDir;
            }
        }
        throw new IllegalArgumentException("Unknown StreamDir value '" + value + "'.");
    }
}
