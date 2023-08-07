package com.zdjizhi.flink.voip.conf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

// Test Configs.
public class TestConfigs {

    // Ratio of valuable data in the generated dataset
    public static final ConfigOption<Integer> VALUABLE_DATA_PROPORTION =
            ConfigOptions.key("valuable.data.ratio")
                    .intType()
                    .defaultValue(40)
                    .withDescription("Ratio of valuable data in the generated dataset.");

    // QPS of generate date record
    public static final ConfigOption<Long> DATA_GENERATE_RATE =
            ConfigOptions.key("data.generate.rate")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("QPS of generate date record.");
}
