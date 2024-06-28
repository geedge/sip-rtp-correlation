package com.geedgenetworks.flink.easy.application;

import com.geedgenetworks.flink.easy.core.Runners;
import org.junit.jupiter.api.Test;

public class ApplicationTest {

    static {
//        System.setProperty("easy.execute.mode", "validate");
        System.setProperty("flink.rest.bind-port", "8081");
//        System.setProperty("flink.rest.flamegraph.enabled", "true");
        System.setProperty("flink.heartbeat.timeout", "1800000");
    }

    public static String discoverConfiguration(final String name) throws Exception {
        var path = String.format("/jobs/%s.yml", name);
        var resource = ApplicationTest.class.getResource(path);
        if (resource == null) {
            // maven
            resource = ApplicationTest.class.getResource(String.format("../classes/%s", path));
        }
        if (resource == null) {
            throw new IllegalArgumentException(
                    String.format("Not found job '%s' in path [%s].", name, path));
        }
        return resource.getPath();
    }

    @Test
    public void testJob() throws Exception {
        Runners.run(discoverConfiguration("job"));
    }
}
