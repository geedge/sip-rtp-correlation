package com.zdjizhi.flink.voip.conf;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FusionConfigurationTest {

    private FusionConfiguration fusionConfiguration;

    @BeforeEach
    public void setUp() {
        final Configuration config;
        config = new Configuration();
        config.setString("prefix_key1", "value1");
        config.setString("prefix_key2", "value2");
        config.setString("other_key", "other_value");

        fusionConfiguration = new FusionConfiguration(config);
    }
    
    @Test
    public void testGetPropertiesWithValidPrefix() {
        String prefix = "prefix_";
        Properties properties = fusionConfiguration.getProperties(prefix);

        assertEquals(2, properties.size());
        assertEquals("value1", properties.getProperty("key1"));
        assertEquals("value2", properties.getProperty("key2"));
    }

    @Test
    public void testGetPropertiesWithInvalidPrefix() {
        String prefix = "invalid_";
        Properties properties = fusionConfiguration.getProperties(prefix);

        assertTrue(properties.isEmpty());
    }

    @Test
    public void testGetPropertiesWithEmptyPrefix() {
        String prefix = "";
        Properties properties = fusionConfiguration.getProperties(prefix);

        assertEquals(3, properties.size());
        assertEquals("value1", properties.getProperty("prefix_key1"));
        assertEquals("value2", properties.getProperty("prefix_key2"));
        assertEquals("other_value", properties.getProperty("other_key"));
    }

    @Test
    public void testGetPropertiesWithNullPrefix() {
        // Null prefix should be treated as an empty prefix
        String prefix = null;
        Properties properties = fusionConfiguration.getProperties(prefix);

        assertEquals(3, properties.size());
        assertEquals("value1", properties.getProperty("prefix_key1"));
        assertEquals("value2", properties.getProperty("prefix_key2"));
        assertEquals("other_value", properties.getProperty("other_key"));
    }
}
