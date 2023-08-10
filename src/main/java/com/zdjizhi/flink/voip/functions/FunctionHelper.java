package com.zdjizhi.flink.voip.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;

/**
 * An interface that provides utility functions for Flink functions.
 *
 * @author chaoc
 * @since 1.0
 */
public interface FunctionHelper extends RichFunction {

    /**
     * Get the global configuration for the current Flink job.
     *
     * @return The global configuration as a Configuration object.
     */

    default Configuration getGlobalConfiguration() {
        final ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        return (Configuration) globalJobParameters;
    }

    /**
     * Get a MapState with the given name, key, and value types.
     *
     * @param name  The name of the MapState.
     * @param key   The class representing the type of keys in the MapState.
     * @param value The class representing the type of values in the MapState.
     * @param <K>   The type of keys in the MapState.
     * @param <V>   The type of values in the MapState.
     * @return The MapState with the given name, key, and value types.
     */
    default <K, V> MapState<K, V> getMapState(String name, Class<K> key, Class<V> value) {
        final MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, key, value);
        return getRuntimeContext().getMapState(descriptor);
    }

    int INTERVAL = 1000 * 15;

    /**
     * Registers the next fire timestamp based on the given TimerService and factor.
     *
     * @param timerService The TimerService to use for registering the timer.
     * @param factor       The factor to adjust the next fire timestamp.
     */
    default void registerNextFireTimestamp(TimerService timerService, int factor) {
        long current = timerService.currentProcessingTime();
        long nextFireTime = (current / INTERVAL + 1) * INTERVAL + factor % INTERVAL;
        timerService.registerProcessingTimeTimer(nextFireTime);
    }

    default void registerIntervalFireTimestamp(TimerService timerService) {
        timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + INTERVAL);
    }
}
