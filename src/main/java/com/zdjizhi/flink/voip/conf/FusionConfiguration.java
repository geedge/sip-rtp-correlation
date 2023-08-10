package com.zdjizhi.flink.voip.conf;

import org.apache.flink.configuration.Configuration;

import java.util.Properties;

/**
 * A wrapper class that extends the Flink `Configuration` to provide utility methods for handling
 * properties with a specific prefix. This class allows retrieving properties that start with the
 * given `prefix` and converts them into a `java.util.Properties` object.
 *
 * @author chaoc
 * @since 1.0
 */

public class FusionConfiguration {
    private final Configuration config;

    public FusionConfiguration(final Configuration config) {
        this.config = config;
    }

    /**
     * Retrieves properties from the underlying `Configuration` instance that start with the specified
     * `prefix`. The properties are then converted into a `java.util.Properties` object and returned.
     *
     * @param prefix The prefix to filter properties.
     * @return A `java.util.Properties` object containing the properties with the specified prefix.
     */
    public Properties getProperties(final String prefix) {
        if (prefix == null) {
            final Properties props = new Properties();
            props.putAll(config.toMap());
            return props;
        }
        return config.toMap()
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .collect(Properties::new, (props, e) ->
                                props.setProperty(e.getKey().substring(prefix.length()), e.getValue()),
                        Properties::putAll);
    }
}
