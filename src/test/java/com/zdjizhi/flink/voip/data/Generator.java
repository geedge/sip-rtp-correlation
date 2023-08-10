package com.zdjizhi.flink.voip.data;

import com.github.javafaker.Faker;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The abstract class Generator<T> serves as the base class for implementing record generators.
 * It provides common functionalities for generating random data and controlling the generation ratio.
 *
 * @param <T> The type of records to be generated.
 * @author chaoc
 * @since 1.0
 */
public abstract class Generator<T> {

    protected final ThreadLocalRandom random;
    /**
     * The ratio of generated records. The total number of records generated will be 100 / ratio.
     */
    protected final int ratio;

    private final Faker faker;

    protected final AtomicReference<T> state;

    protected final ObjectMapper mapper;

    /**
     * Creates a new Generator with the given ratio.
     *
     * @param ratio The ratio of generated records. The total number of records generated will be 100 / ratio.
     */
    public Generator(final int ratio) {
        this.ratio = ratio;
        this.faker = new Faker();
        this.random = ThreadLocalRandom.current();
        this.state = new AtomicReference<>();
        this.mapper = new ObjectMapper();
    }

    /**
     * Generates the next record based on the specified ratio and the current state.
     * It randomly selects whether to generate a new record or return the last generated record.
     *
     * @return The next generated record of type T.
     */
    public abstract T next();

    /**
     * Generates a new record of type T.
     *
     * @return The newly generated record of type T.
     */
    protected abstract T generate();

    /**
     * Performs post-processing on the generated record of type T.
     * Subclasses can override this method to modify the generated record before returning it.
     *
     * @param v The generated record of type T.
     * @return The post-processed record of type T.
     */
    protected abstract T afterState(T v);

    /**
     * Generates a random IP address (either IPv4 or IPv6) .
     *
     * @return A randomly generated IP address as a string.
     */
    public final String nextIp() {
        if (random.nextBoolean()) {
            return faker.internet().ipV4Address();
        }
        return faker.internet().ipV6Address();
    }

    /**
     * Generates a random ID number.
     *
     * @return A randomly generated ID number as a string.
     */
    public final String nextId() {
        return faker.idNumber().valid();
    }

    /**
     * Generates a random port number within the range of 0 to 65535 (inclusive).
     *
     * @return A randomly generated port number as an integer.
     */
    public final int nextPort() {
        return random.nextInt(65535);
    }
}

